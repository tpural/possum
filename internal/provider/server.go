package provider

import (
	"context"
	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/tpural/possum/internal/externalgrpc/protos"
)

const gpuLabel = "possum.dev/gpu"

// Server adapts a Fleet to the Cluster Autoscaler externalgrpc API.
type Server struct {
	protos.UnimplementedCloudProviderServer
	fleet *Fleet
}

// NewServer returns a Server backed by f.
func NewServer(f *Fleet) *Server { return &Server{fleet: f} }

// ReadyInterceptor rejects calls until the fleet has completed its first
// reconcile, so the autoscaler never sees an empty, half-loaded fleet.
func ReadyInterceptor(f *Fleet) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		if !f.Ready() {
			return nil, status.Error(codes.Unavailable, "possum is starting")
		}
		return next(ctx, req)
	}
}

func (s *Server) NodeGroups(context.Context, *protos.NodeGroupsRequest) (*protos.NodeGroupsResponse, error) {
	resp := &protos.NodeGroupsResponse{}
	for _, g := range s.fleet.Groups() {
		resp.NodeGroups = append(resp.NodeGroups, &protos.NodeGroup{
			Id:      g.Name,
			MinSize: int32(g.MinSize),
			MaxSize: int32(g.MaxSize),
		})
	}
	return resp, nil
}

func (s *Server) NodeGroupForNode(_ context.Context, req *protos.NodeGroupForNodeRequest) (*protos.NodeGroupForNodeResponse, error) {
	n := req.GetNode()
	name := s.fleet.GroupFor(n.GetName(), n.GetProviderID())
	for _, g := range s.fleet.Groups() {
		if g.Name == name {
			return &protos.NodeGroupForNodeResponse{NodeGroup: &protos.NodeGroup{
				Id:      g.Name,
				MinSize: int32(g.MinSize),
				MaxSize: int32(g.MaxSize),
			}}, nil
		}
	}
	return &protos.NodeGroupForNodeResponse{NodeGroup: &protos.NodeGroup{}}, nil
}

func (s *Server) GPULabel(context.Context, *protos.GPULabelRequest) (*protos.GPULabelResponse, error) {
	return &protos.GPULabelResponse{Label: gpuLabel}, nil
}

func (s *Server) GetAvailableGPUTypes(context.Context, *protos.GetAvailableGPUTypesRequest) (*protos.GetAvailableGPUTypesResponse, error) {
	return &protos.GetAvailableGPUTypesResponse{}, nil
}

func (s *Server) Cleanup(context.Context, *protos.CleanupRequest) (*protos.CleanupResponse, error) {
	return &protos.CleanupResponse{}, nil
}

func (s *Server) Refresh(context.Context, *protos.RefreshRequest) (*protos.RefreshResponse, error) {
	s.fleet.Trigger()
	return &protos.RefreshResponse{}, nil
}

func (s *Server) NodeGroupTargetSize(_ context.Context, req *protos.NodeGroupTargetSizeRequest) (*protos.NodeGroupTargetSizeResponse, error) {
	n, err := s.fleet.TargetSize(req.GetId())
	if err != nil {
		return nil, grpcErr(err)
	}
	return &protos.NodeGroupTargetSizeResponse{TargetSize: int32(n)}, nil
}

func (s *Server) NodeGroupIncreaseSize(ctx context.Context, req *protos.NodeGroupIncreaseSizeRequest) (*protos.NodeGroupIncreaseSizeResponse, error) {
	if err := s.fleet.IncreaseSize(ctx, req.GetId(), int(req.GetDelta())); err != nil {
		return nil, grpcErr(err)
	}
	return &protos.NodeGroupIncreaseSizeResponse{}, nil
}

func (s *Server) NodeGroupDeleteNodes(ctx context.Context, req *protos.NodeGroupDeleteNodesRequest) (*protos.NodeGroupDeleteNodesResponse, error) {
	names := make([]string, 0, len(req.GetNodes()))
	for _, n := range req.GetNodes() {
		names = append(names, s.fleet.Resolve(n.GetName(), n.GetProviderID()))
	}
	if err := s.fleet.DeleteNodes(ctx, req.GetId(), names); err != nil {
		return nil, grpcErr(err)
	}
	return &protos.NodeGroupDeleteNodesResponse{}, nil
}

func (s *Server) NodeGroupDecreaseTargetSize(ctx context.Context, req *protos.NodeGroupDecreaseTargetSizeRequest) (*protos.NodeGroupDecreaseTargetSizeResponse, error) {
	if err := s.fleet.DecreaseTargetSize(ctx, req.GetId(), int(req.GetDelta())); err != nil {
		return nil, grpcErr(err)
	}
	return &protos.NodeGroupDecreaseTargetSizeResponse{}, nil
}

func (s *Server) NodeGroupNodes(_ context.Context, req *protos.NodeGroupNodesRequest) (*protos.NodeGroupNodesResponse, error) {
	if _, err := s.fleet.group(req.GetId()); err != nil {
		return nil, grpcErr(err)
	}
	resp := &protos.NodeGroupNodesResponse{}
	for _, m := range s.fleet.Members() {
		if m.Group != req.GetId() {
			continue
		}
		st := &protos.InstanceStatus{}
		switch m.Phase {
		case PhaseRunning:
			st.InstanceState = protos.InstanceStatus_instanceRunning
		case PhaseBooting:
			st.InstanceState = protos.InstanceStatus_instanceCreating
			if m.Message == msgBootTimeout {
				st.ErrorInfo = &protos.InstanceErrorInfo{ErrorCode: "BootTimeout", ErrorMessage: m.Message, InstanceErrorClass: 99}
			}
		case PhaseShuttingDown:
			st.InstanceState = protos.InstanceStatus_instanceDeleting
		default:
			continue
		}
		resp.Instances = append(resp.Instances, &protos.Instance{Id: m.ProviderID(), Status: st})
	}
	return resp, nil
}

func (s *Server) NodeGroupTemplateNodeInfo(ctx context.Context, req *protos.NodeGroupTemplateNodeInfoRequest) (*protos.NodeGroupTemplateNodeInfoResponse, error) {
	node, err := s.fleet.TemplateNode(ctx, req.GetId())
	if err != nil {
		return nil, grpcErr(err)
	}
	b, err := node.Marshal()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &protos.NodeGroupTemplateNodeInfoResponse{NodeBytes: b}, nil
}

// NodeGroupGetOptions overrides the autoscaler's defaults for groups that set their own.
func (s *Server) NodeGroupGetOptions(_ context.Context, req *protos.NodeGroupAutoscalingOptionsRequest) (*protos.NodeGroupAutoscalingOptionsResponse, error) {
	g, err := s.fleet.group(req.GetId())
	if err != nil {
		return nil, grpcErr(err)
	}
	if g.cfg.ScaleDownUnneededTime.Duration == 0 || req.GetDefaults() == nil {
		return nil, status.Error(codes.Unimplemented, "no per-group options")
	}
	opts := proto.Clone(req.GetDefaults()).(*protos.NodeGroupAutoscalingOptions)
	opts.ScaleDownUnneededDuration = durationpb.New(g.cfg.ScaleDownUnneededTime.Duration)
	return &protos.NodeGroupAutoscalingOptionsResponse{NodeGroupAutoscalingOptions: opts}, nil
}

func grpcErr(err error) error {
	if errors.Is(err, ErrNoTemplate) {
		return status.Error(codes.Unimplemented, err.Error())
	}
	return status.Error(codes.FailedPrecondition, err.Error())
}
