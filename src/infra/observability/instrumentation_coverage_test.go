package observability_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"unicode"
)

const featuresRoot = "../../features"
const handlersRoot = "../../api/rest/v1/handlers"

var exemptFunctionNames = map[string]bool{"Error": true, "String": true, "Close": true}

func goFiles(t *testing.T, pattern string) []string {
	t.Helper()
	matches, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatal(err)
	}
	files := make([]string, 0, len(matches))
	for _, match := range matches {
		if !strings.HasSuffix(match, "_test.go") {
			files = append(files, match)
		}
	}
	return files
}

func parseFile(t *testing.T, path string) *ast.File {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return file
}

func exportedFunctions(file *ast.File) []*ast.FuncDecl {
	functions := make([]*ast.FuncDecl, 0)
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil || !unicode.IsUpper(rune(function.Name.Name[0])) {
			continue
		}
		if strings.HasPrefix(function.Name.Name, "New") || exemptFunctionNames[function.Name.Name] {
			continue
		}
		functions = append(functions, function)
	}
	return functions
}

func callsSelector(body *ast.BlockStmt, names ...string) bool {
	wanted := make(map[string]bool, len(names))
	for _, name := range names {
		wanted[name] = true
	}
	found := false
	ast.Inspect(body, func(node ast.Node) bool {
		if selector, ok := node.(*ast.SelectorExpr); ok && wanted[selector.Sel.Name] {
			found = true
		}
		return !found
	})
	return found
}

func returnsError(function *ast.FuncDecl) bool {
	results := function.Type.Results
	if results == nil || len(results.List) == 0 {
		return false
	}
	identifier, ok := results.List[len(results.List)-1].Type.(*ast.Ident)
	return ok && identifier.Name == "error"
}

func swallowedCalls(file *ast.File) []string {
	violations := make([]string, 0)
	ast.Inspect(file, func(node ast.Node) bool {
		assign, ok := node.(*ast.AssignStmt)
		if !ok || assign.Tok != token.ASSIGN || len(assign.Rhs) != 1 {
			return true
		}
		for _, left := range assign.Lhs {
			if identifier, isIdent := left.(*ast.Ident); !isIdent || identifier.Name != "_" {
				return true
			}
		}
		if call, isCall := assign.Rhs[0].(*ast.CallExpr); isCall {
			if selector, isSelector := call.Fun.(*ast.SelectorExpr); isSelector && (selector.Sel.Name == "Close" || selector.Sel.Name == "Rollback") {
				return true
			}
			violations = append(violations, "swallowed error")
		}
		return true
	})
	return violations
}

func TestAllTwentyFiveFeaturesAreDiscovered(t *testing.T) {
	entries, err := os.ReadDir(featuresRoot)
	if err != nil {
		t.Fatal(err)
	}
	features := 0
	for _, entry := range entries {
		if entry.IsDir() {
			features++
		}
	}
	if features != 25 {
		t.Fatalf("expected 25 features, found %d; update the coverage guard when the feature list changes", features)
	}
}

func TestEveryServiceMethodIsSpannedAndRecordsItsErrors(t *testing.T) {
	violations := make([]string, 0)
	for _, path := range goFiles(t, featuresRoot+"/*/service/*.go") {
		for _, function := range exportedFunctions(parseFile(t, path)) {
			spanned := callsSelector(function.Body, "StartSpan", "StartLayerSpan", "TraceEnter")
			if !spanned {
				violations = append(violations, path+": "+function.Name.Name+" has no span")
				continue
			}
			if returnsError(function) && !callsSelector(function.Body, "RecordError", "TraceEnter", "Trace") {
				violations = append(violations, path+": "+function.Name.Name+" does not record its error path")
			}
		}
	}
	reportViolations(t, violations)
}

func TestEveryDomainSchemaAndAdapterFunctionIsTraced(t *testing.T) {
	patterns := []string{
		"/*/domain/rules/*.go", "/*/domain/machines/*.go", "/*/domain/workflows/*.go",
		"/*/infrastructure/adapters/*.go", "/*/contracts/*.schema.go",
	}
	violations := make([]string, 0)
	for _, pattern := range patterns {
		for _, path := range goFiles(t, featuresRoot+pattern) {
			for _, function := range exportedFunctions(parseFile(t, path)) {
				if !callsSelector(function.Body, "TraceEnter") {
					violations = append(violations, path+": "+function.Name.Name+" is not traced")
				}
			}
		}
	}
	reportViolations(t, violations)
}

func TestEveryHandlerMethodIsSpanned(t *testing.T) {
	violations := make([]string, 0)
	for _, path := range goFiles(t, handlersRoot+"/*.go") {
		for _, function := range exportedFunctions(parseFile(t, path)) {
			if function.Recv == nil {
				continue
			}
			if !callsSelector(function.Body, "StartSpan", "StartLayerSpan", "TraceEnter") {
				violations = append(violations, path+": "+function.Name.Name+" has no span")
			}
		}
	}
	reportViolations(t, violations)
}

func TestNoSilentlySwallowedErrorsRemain(t *testing.T) {
	patterns := []string{
		featuresRoot + "/*/service/*.go", featuresRoot + "/*/infrastructure/adapters/*.go", featuresRoot + "/*/domain/rules/*.go",
		featuresRoot + "/*/contracts/*.schema.go", handlersRoot + "/*.go", "../../api/rest/v1/routers/*.go",
	}
	violations := make([]string, 0)
	for _, pattern := range patterns {
		for _, path := range goFiles(t, pattern) {
			for range swallowedCalls(parseFile(t, path)) {
				violations = append(violations, path+": error assigned to _ without NoteSwallowed")
			}
		}
	}
	reportViolations(t, violations)
}

func TestServerUsesTheTracedDatabaseDriver(t *testing.T) {
	content, err := os.ReadFile("../chassis/runner.go")
	if err != nil || !strings.Contains(string(content), "OpenTracedPostgres") {
		t.Fatal("the chassis must open PostgreSQL through observability.OpenTracedPostgres so every SQL statement is traced")
	}
}

func reportViolations(t *testing.T, violations []string) {
	t.Helper()
	if len(violations) == 0 {
		return
	}
	sort.Strings(violations)
	if len(violations) > 25 {
		t.Fatalf("%d violations, first 25:\n%s", len(violations), strings.Join(violations[:25], "\n"))
	}
	t.Fatalf("%d violations:\n%s", len(violations), strings.Join(violations, "\n"))
}
