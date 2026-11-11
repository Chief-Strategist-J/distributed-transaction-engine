package config

const defaultMaxConcurrentBranches = 32

type ParallelConfig struct {
	MaxConcurrentBranches int `yaml:"max_concurrent_branches"`
}

func (p *ParallelConfig) ApplyEnvironment() {
	p.MaxConcurrentBranches = positiveInt("PARALLEL_MAX_CONCURRENT_BRANCHES", p.MaxConcurrentBranches)
}

func (p *ParallelConfig) ConcurrencyLimit() int {
	if p.MaxConcurrentBranches <= 0 {
		return defaultMaxConcurrentBranches
	}
	return p.MaxConcurrentBranches
}
