package main

import (
	"errors"
	"fmt"
	"reflect"
)

func i2s(data interface{}, out interface{}) error {
	outValue := reflect.ValueOf(out).Elem()
	outType := outValue.Type()
	_ = outType//TODO

	dataMap, ok := data.(map[string]interface{})
	if !ok {
		return errors.New("data must be map[string]interface{}")
	}
	for key, val := range dataMap {
		field := outValue.FieldByName(key)
		if !field.IsValid() {
			return fmt.Errorf("could not find field %v", key)
		}

		switch field.Type().Name() {
		case "string":
			valStr, ok := val.(string)
			if !ok {
				return fmt.Errorf("field %v must be string", key)
			}
			field.SetString(valStr)
		case "bool":
			valBool, ok := val.(bool)
			if !ok {
				return fmt.Errorf("field %v must be bool", key)
			}
			field.SetBool(valBool)
		case "int":
			valFloat, ok := val.(float64)
			if !ok {
				return fmt.Errorf("field %v must be number", key)
			}
			field.SetInt(int64(valFloat))
		default:
			fmt.Println(field.Type().Name() )

		}

		_ = val //TODO
	}

	return nil
}

