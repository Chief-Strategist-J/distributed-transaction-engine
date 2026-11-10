package workflows

import "distributed-transaction-engine/src/infra/observability"

const (
	StepExportLoadJob    = "EXPORT_LOAD_JOB"
	StepExportStart      = "EXPORT_START"
	StepExportRevalidate = "EXPORT_REVALIDATE"
	StepExportFetchChunk = "EXPORT_FETCH_CHUNK"
	StepExportSealChunk  = "EXPORT_SEAL_CHUNK"
	StepExportFinalize   = "EXPORT_FINALIZE"
)

func ExportGenerationSteps() []Step {
	defer observability.TraceEnter(nil)()
	return []Step{
		{ID: StepExportLoadJob},
		{ID: StepExportStart, DependsOn: []string{StepExportLoadJob}},
		{ID: StepExportRevalidate, DependsOn: []string{StepExportStart}},
		{ID: StepExportFetchChunk, DependsOn: []string{StepExportRevalidate}},
		{ID: StepExportSealChunk, DependsOn: []string{StepExportFetchChunk}},
		{ID: StepExportFinalize, DependsOn: []string{StepExportSealChunk}},
	}
}
