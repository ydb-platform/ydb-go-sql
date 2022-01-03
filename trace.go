package ydb

import "github.com/ydb-platform/ydb-go-sdk/v3/trace"

type TraceDetails trace.Details

const (
	TraceDriverSystemEvents      = TraceDetails(trace.DriverSystemEvents)
	TraceDriverClusterEvents     = TraceDetails(trace.DriverClusterEvents)
	TraceDriverNetEvents         = TraceDetails(trace.DriverNetEvents)
	TraceDriverCoreEvents        = TraceDetails(trace.DriverCoreEvents)
	TraceDriverCredentialsEvents = TraceDetails(trace.DriverCredentialsEvents)
	TraceDriverDiscoveryEvents   = TraceDetails(trace.DriverDiscoveryEvents)

	TraceTableSessionLifeCycleEvents     = TraceDetails(trace.TableSessionLifeCycleEvents)
	TraceTableSessionQueryInvokeEvents   = TraceDetails(trace.TableSessionQueryInvokeEvents)
	TraceTableSessionQueryStreamEvents   = TraceDetails(trace.TableSessionQueryStreamEvents)
	TraceTableSessionTransactionEvents   = TraceDetails(trace.TableSessionTransactionEvents)
	TraceTablePoolLifeCycleEvents        = TraceDetails(trace.TablePoolLifeCycleEvents)
	TraceTablePoolRetryEvents            = TraceDetails(trace.TablePoolRetryEvents)
	TraceTablePoolSessionLifeCycleEvents = TraceDetails(trace.TablePoolSessionLifeCycleEvents)
	TraceTablePoolAPIEvents              = TraceDetails(trace.TablePoolAPIEvents)

	TraceDriverConnEvents        = TraceDetails(trace.DriverConnEvents)
	TraceTableSessionQueryEvents = TraceDetails(trace.TableSessionQueryEvents)
	TraceTableSessionEvents      = TraceDetails(trace.TableSessionEvents)
	TraceTablePoolEvents         = TraceDetails(trace.TablePoolEvents)
	TraceDetailsAll              = TraceDetails(trace.DetailsAll)
)
