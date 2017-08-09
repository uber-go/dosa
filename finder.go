// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package dosa

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/pkg/errors"
)

// FindEntities finds all entities in the given file paths. An error is
// returned if there are naming colisions, otherwise, return a slice of
// warnings (or nil).
func FindEntities(paths, excludes []string) ([]*Table, []error, error) {
	var entities []*Table
	var warnings []error
	for _, path := range paths {
		fileSet := token.NewFileSet()
		packages, err := parser.ParseDir(fileSet, path, func(fileInfo os.FileInfo) bool {
			if len(excludes) == 0 {
				return true
			}
			for _, exclude := range excludes {
				if matched, _ := filepath.Match(exclude, fileInfo.Name()); matched {
					return false
				}
			}
			return true
		}, 0)
		if err != nil {
			return nil, nil, err
		}
		erv := new(EntityRecordingVisitor)
		for _, pkg := range packages { // go through all the packages
			for _, file := range pkg.Files { // go through all the files
				packagePrefix, hasDosa := findDosaPackage(file)
				//if erv.PackageName != "" { // skip packages that don't import 'dosa'
				if hasDosa {
					erv.PackagePrefix = packagePrefix
					for _, decl := range file.Decls { // go through all the declarations
						ast.Walk(erv, decl)
					}
				}
			}
		}
		entities = append(entities, erv.Entities...)
		warnings = append(warnings, erv.Warnings...)
	}

	return entities, warnings, nil
}

// DosaPackageName is the name of the dosa package, fully qualified and quoted
const DosaPackageName = `"github.com/uber-go/dosa"`

func findDosaPackage(file *ast.File) (string, bool) {
	// look for the case where we import dosa
	for _, impspec := range file.Imports {
		if impspec.Path.Value == DosaPackageName {
			// impspec.Name is nil when not renamed,
			// so we use the default "dosa"
			if impspec.Name == nil {
				return "dosa", true
			}
			// renamed case
			return impspec.Name.Name, true
		}
	}
	if file.Name.Name == "dosa" {
		// special case: our package is 'dosa' so no prefix is required
		return "", true
	}
	// this file doesn't have any references to dosa, so skip it
	return "", false
}

// EntityRecordingVisitor is a visitor that records entities it finds
// It also keeps track of all failed entities that pass the basic "looks like a DOSA object" test
// (see isDosaEntity to understand that test)
type EntityRecordingVisitor struct {
	Entities      []*Table
	Warnings      []error
	PackagePrefix string
}

// Visit records all the entities seen into the EntityRecordingVisitor structure
func (f *EntityRecordingVisitor) Visit(n ast.Node) ast.Visitor {
	switch n := n.(type) {
	case *ast.File, *ast.Package, *ast.BlockStmt, *ast.DeclStmt, *ast.FuncDecl, *ast.GenDecl:
		return f
	case *ast.TypeSpec:
		if structType, ok := n.Type.(*ast.StructType); ok {
			// look for a Entity with a dosa annotation
			if isDosaEntity(structType) {
				table, err := tableFromStructType(n.Name.Name, structType, f.PackagePrefix)
				if err == nil {
					f.Entities = append(f.Entities, table)
				} else {
					f.Warnings = append(f.Warnings, err)
				}
			}
		}
	}
	return nil
}

// isDosaEntity is a sanity check so that only objects that are probably supposed to be dosa
// annotated objects will generate warnings. The rules for that are:
//  - must have some fields
//  - the first field should be of type Entity
//    TODO: Really any field could be type Entity, but we currently do not have this case

func isDosaEntity(structType *ast.StructType) bool {
	// structures with no fields cannot be dosa entities
	if len(structType.Fields.List) < 1 {
		return false
	}

	// the first field should be a DOSA Entity type
	candidateEntityField := structType.Fields.List[0]
	if identifier, ok := candidateEntityField.Type.(*ast.Ident); ok {
		if identifier.Name != entityName {
			return false
		}
	}

	// and should have a DOSA tag
	if candidateEntityField.Tag == nil || candidateEntityField.Tag.Kind != token.STRING {
		return false
	}
	entityTag := reflect.StructTag(strings.Trim(candidateEntityField.Tag.Value, "`"))
	if entityTag.Get(dosaTagKey) == "" {
		return false
	}

	return true
}

func parseASTType(expr ast.Expr) (string, error) {
	var kind string
	var err error
	switch typeName := expr.(type) {
	case *ast.Ident:
		kind = typeName.Name
		// not an Entity type, perhaps another primitive type
	case *ast.ArrayType:
		// only dosa allowed array type is []byte
		if typeName, ok := typeName.Elt.(*ast.Ident); ok {
			if typeName.Name == "byte" {
				kind = "[]byte"
			}
		}
	case *ast.SelectorExpr:
		// only dosa allowed selector is time.Time
		if innerName, ok := typeName.X.(*ast.Ident); ok {
			kind = innerName.Name + "." + typeName.Sel.Name
		}
	case *ast.StarExpr:
		// pointer types
		// need to recursively parse the type
		kind, err = parseASTType(typeName.X)
		kind = "*" + kind
	default:
		err = fmt.Errorf("Unexpected field type: %v", typeName)
	}

	return kind, err
}

// tableFromStructType takes an ast StructType and converts it into a Table object
func tableFromStructType(structName string, structType *ast.StructType, packagePrefix string) (*Table, error) {
	normalizedName, err := NormalizeName(structName)
	if err != nil {
		// TODO: This isn't correct, someone could override the name later
		return nil, errors.Wrapf(err, "struct name is invalid")
	}

	t := &Table{
		StructName: structName,
		EntityDefinition: EntityDefinition{
			Name:    normalizedName,
			Columns: []*ColumnDefinition{},
			Indexes: map[string]*IndexDefinition{},
		},
		ColToField: map[string]string{},
		FieldToCol: map[string]string{},
	}
	for _, field := range structType.Fields.List {
		var dosaTag string
		if field.Tag != nil {
			entityTag := reflect.StructTag(strings.Trim(field.Tag.Value, "`"))
			dosaTag = strings.TrimSpace(entityTag.Get(dosaTagKey))
		}
		if dosaTag == "-" { // skip explicitly ignored fields
			continue
		}

		kind, err := parseASTType(field.Type)
		if err != nil {
			return nil, err
		}

		if kind == packagePrefix+"."+entityName || (packagePrefix == "" && kind == entityName) {
			var err error
			if t.EntityDefinition.Name, t.Key, err = parseEntityTag(structName, dosaTag); err != nil {
				return nil, err
			}
		} else {
			for _, fieldName := range field.Names {
				name := fieldName.Name
				if kind == packagePrefix+"."+indexName || (packagePrefix == "" && kind == indexName) {
					indexName, indexKey, err := parseIndexTag(name, dosaTag)
					if err != nil {
						return nil, err
					}
					if _, exist := t.Indexes[indexName]; exist {
						return nil, errors.Errorf("index name is duplicated: %s", indexName)
					}
					t.Indexes[indexName] = &IndexDefinition{Key: indexKey}
				} else {
					firstRune, _ := utf8.DecodeRuneInString(name)
					if unicode.IsLower(firstRune) {
						// skip unexported fields
						continue
					}
					typ, isPointer := stringToDosaType(kind, packagePrefix)
					if typ == Invalid {
						return nil, fmt.Errorf("Column %q has invalid type %q", name, kind)
					}
					cd, err := parseField(typ, isPointer, name, dosaTag)
					if err != nil {
						return nil, errors.Wrapf(err, "column %q", name)
					}
					t.Columns = append(t.Columns, cd)
					t.ColToField[cd.Name] = name
					t.FieldToCol[name] = cd.Name
				}
			}

			if len(field.Names) == 0 {
				if kind == packagePrefix+"."+indexName || (packagePrefix == "" && kind == indexName) {
					indexName, indexKey, err := parseIndexTag("", dosaTag)
					if err != nil {
						return nil, err
					}
					if _, exist := t.Indexes[indexName]; exist {
						return nil, errors.Errorf("index name is duplicated: %s", indexName)
					}
					t.Indexes[indexName] = &IndexDefinition{Key: indexKey}
				}
			}
		}
	}

	if t.Key == nil {
		return nil, errors.Errorf("cannot find dosa.Entity in object %s", t.StructName)
	}

	translateKeyName(t)
	if err := t.EnsureValid(); err != nil {
		return nil, errors.Wrap(err, "failed to parse dosa object")
	}
	return t, nil
}

func stringToDosaType(inType, pkg string) (Type, bool) {

	// Append a dot if the package suffix doesn't already have one.
	if pkg != "" && !strings.HasSuffix(pkg, ".") {
		pkg += "."
	}

	switch inType {
	case "string":
		return String, false
	case "[]byte":
		return Blob, false
	case "bool":
		return Bool, false
	case "int32":
		return Int32, false
	case "int64":
		return Int64, false
	case "float64":
		return Double, false
	case "time.Time":
		return Timestamp, false
	case "UUID", pkg + "UUID":
		return TUUID, false
	case "*string":
		return String, true
	case "*bool":
		return Bool, true
	case "*int32":
		return Int32, true
	case "*int64":
		return Int64, true
	case "*float64":
		return Double, true
	case "*time.Time":
		return Timestamp, true
	case "*UUID", "*" + pkg + "UUID":
		return TUUID, true
	default:
		return Invalid, false
	}
}
