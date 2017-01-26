package dosa

import (
	"fmt"
	"github.com/pkg/errors"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"unicode"
	"unicode/utf8"
)

// FindEntities finds all entities in a directory
// Returns a slice of warnings (or nil)
func FindEntities(path string) ([]Table, []error) {
	fileSet := token.NewFileSet()
	packages, _ := parser.ParseDir(fileSet, path, nil, 0)
	erv := new(EntityRecordingVisitor)
	for _, pkg := range packages { // go through all the packages
		for _, file := range pkg.Files { // go through all the files
			for _, decl := range file.Decls { // go through all the declarations
				ast.Walk(erv, decl)
			}
		}
	}
	return erv.Entities, erv.Warnings
}

// EntityRecordingVisitor is a visitor that records entities it finds
// It also keeps track of all failed entities that pass the basic "looks like a DOSA object" test
// (see isDosaEntity to understand that test)
type EntityRecordingVisitor struct {
	Entities []Table
	Warnings []error
}

// isDosaEntity is a sanity check so that only objects that are probably supposed to be dosa
// annotated objects will generate warnings. The rules for that are:
//  - must have some fields
//  - the first field should be of type Entity

func isDosaEntity(structType *ast.StructType) bool {
	// structures with no fields cannot be dosa entities
	if structType.Fields == nil {
		return false
	}
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

// tableFromStructType takes an ast StructType and converts it into a Table object
func tableFromStructType(structName string, structType *ast.StructType) (*Table, error) {
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
		var kind string
		switch typeName := field.Type.(type) {
		case *ast.Ident:
			kind = typeName.Name
			// not an Entity type, perhaps another primative type
		case *ast.ArrayType:
			// only dosa allowed array type is []byte
			if typeName, ok := typeName.Elt.(*ast.Ident); ok {
				if typeName.Name == "byte" {
					kind = "[]byte"
				}
			}
		case *ast.SelectorExpr:
			// only dosa allowed selector is time.Time
			if typeName, ok := typeName.X.(*ast.Ident); ok {
				if typeName.Name == "time" {
					kind = "time.Time"
				}
			}
		default:
			fmt.Printf("got type %T %s\n", field.Type, field.Type)
		}
		if kind == entityName {
			var err error
			if t.EntityDefinition.Name, t.Key, err = ParseEntityTag(structName, dosaTag); err != nil {
				return nil, err
			}
		} else {
			fieldName := field.Names[0].Name
			firstRune, _ := utf8.DecodeRuneInString(fieldName)
			if unicode.IsLower(firstRune) {
				// skip unexported fields
				continue
			}
			typ := stringToDosaType(kind)
			if typ == Invalid {
				return nil, fmt.Errorf("Column %q has invalid type %q", fieldName, kind)
			}
			cd, err := parseField(typ, fieldName, dosaTag)
			if err != nil {
				return nil, errors.Wrapf(err, "column %q", fieldName)
			}
			t.Columns = append(t.Columns, cd)
			t.ColToField[cd.Name] = fieldName
			t.FieldToCol[fieldName] = cd.Name
		}
	}
	translateKeyName(t)
	if err := t.EnsureValid(); err != nil {
		return nil, errors.Wrap(err, "failed to parse dosa object")
	}
	return t, nil
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
				table, err := tableFromStructType(n.Name.Name, structType)
				if err == nil {
					f.Entities = append(f.Entities, *table)
				} else {
					if f.Warnings == nil {
						f.Warnings = make([]error, 0)
					}
					f.Warnings = append(f.Warnings, err)
				}
			}
		}
	}
	return nil
}
func stringToDosaType(inType string) Type {
	switch inType {
	case "string":
		return String
	case "[]byte":
		return Blob
	case "bool":
		return Bool
	case "int32":
		return Int32
	case "int64":
		return Int64
	case "float64":
		return Double
	case "time.Time":
		return Timestamp
	case "UUID":
		return TUUID
	default:
		return Invalid
	}
}
