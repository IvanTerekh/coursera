package main

import (
	"errors"
	"fmt"
	"reflect"
)

func i2s(data interface{}, outPtr interface{}) error {
	outPtrValue := reflect.ValueOf(outPtr)
	if outPtrValue.Kind() != reflect.Ptr {
		return errors.New("a pointer expected")
	}
	out := outPtrValue.Elem()

	switch out.Kind() {
	case reflect.String:
		valStr, ok := data.(string)
		if !ok {
			return fmt.Errorf("%v must be string", data)
		}
		out.SetString(valStr)
	case reflect.Bool:
		valBool, ok := data.(bool)
		if !ok {
			return fmt.Errorf("%v must be bool", data)
		}
		out.SetBool(valBool)
	case reflect.Int:
		valFloat, ok := data.(float64)
		if !ok {
			return fmt.Errorf("%v must be number", data)
		}
		out.SetInt(int64(valFloat))
	case reflect.Struct:
		dataMap, ok := data.(map[string]interface{})
		if !ok {
			return fmt.Errorf("%v must be map[string]interface{}", data)
		}
		for key, val := range dataMap {
			dest := reflect.New(out.FieldByName(key).Type()).Interface()
			err := i2s(val, dest)
			if err != nil {
				return fmt.Errorf("could not convert %v to struct: %v", data, err)
			}
			out.FieldByName(key).Set(reflect.ValueOf(dest).Elem())
		}
	case reflect.Slice:
		vals, ok := data.([]interface{})
		if !ok {
			return errors.New("%v must be a slice")
		}
		var dests = reflect.New(out.Type()).Elem()
		for _, val := range vals {
			dest := reflect.New(out.Type().Elem()).Interface()
			err := i2s(val, dest)
			if err != nil {
				return fmt.Errorf("could not convert %v to struct: %v", val, err)
			}
			dests = reflect.Append(dests, reflect.ValueOf(dest).Elem())
		}
		out.Set(dests)
	default:
		return fmt.Errorf("unknown type of field %v: %v", out, out.Type().Name())

	}

	return nil
}
