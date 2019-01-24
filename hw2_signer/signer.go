package main

import (
	"sync"
	"strconv"
	"strings"
	"sort"
	"fmt"
)

type syncMd5Signer sync.Mutex

func (mx *syncMd5Signer) sign(data string) string {
	(*sync.Mutex)(mx).Lock()
	defer (*sync.Mutex)(mx).Unlock()
	return DataSignerMd5(data)
}

var syncMd5 = syncMd5Signer{}

func (j *job) do(in, out chan interface{}, wg *sync.WaitGroup) {
	(func(in, out chan interface{}))(*j)(in, out)
	close(out)
	wg.Done()
}

func ExecutePipeline(jobs ...job) {
	var in, out chan interface{} = nil, make(chan interface{})
	wg := &sync.WaitGroup{}

	n := len(jobs)
	wg.Add(n)

	go jobs[0].do(in, out, wg)

	for i := 1; i < n; i++ {
		in = out
		out = make(chan interface{})
		go jobs[i].do(in, out, wg)
	}
	wg.Wait()
}

// SingleHash считает значение crc32(data)+"~"+crc32(md5(data))
//( конкатенация двух строк через ~), где data - то что пришло
// на вход (по сути - числа из первой функции).
func SingleHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	for data := range in {
		data := data
		wg.Add(1)
		go func() {
			asyncSingleHash(data, out)
			wg.Done()
		}()
	}
	wg.Wait()
}

func asyncSingleHash(data interface{}, out chan<- interface{}) {
	dataInt, ok := data.(int)
	if !ok {
		panic(fmt.Errorf("could not convert to int: %#v", data))
	}
	dataStr := strconv.Itoa(dataInt)
	wg := &sync.WaitGroup{}
	wg.Add(2)
	hash1 := asyncCrc32(dataStr, wg)
	hash2 := asyncCrc32(syncMd5.sign(dataStr), wg)
	wg.Wait()
	hash := *hash1 + "~" + *hash2
	out <- hash
}

func asyncCrc32(data string, wg *sync.WaitGroup) *string {
	hash := new(string)
	go func() {
		*hash = DataSignerCrc32(data)
		wg.Done()
	}()
	return hash
}

// MultiHash считает значение crc32(th+data)) (конкатенация цифры,
// приведённой к строке и строки), где th=0..5 ( т.е. 6 хешей на
// каждое входящее значение ), потом берёт конкатенацию результатов
// в порядке расчета (0..5), где data - то что пришло на вход
// (и ушло на выход из SingleHash)
func MultiHash(in, out chan interface{}) {
	wgMultiHash := &sync.WaitGroup{}
	for data := range in {
		data := data
		wgMultiHash.Add(1)
		go func() {
			asyncMultiHash(data, out)
			wgMultiHash.Done()
		}()
	}
	wgMultiHash.Wait()
}

func asyncMultiHash(data interface{}, out chan<- interface{}) {
	n := 6
	dataStr, ok := data.(string)
	if !ok {
		panic(fmt.Errorf("could not convert to string: %#v", data))
	}
	hashes := make([]*string, n)
	wg := &sync.WaitGroup{}
	wg.Add(n)
	for i := range hashes {
		iStr := strconv.Itoa(i)
		hashes[i] = asyncCrc32(iStr+dataStr, wg)
	}
	res := strings.Builder{}
	wg.Wait()
	for _, hash := range hashes {
		res.WriteString(*hash)
	}
	out <- res.String()
}

// CombineResults получает все результаты, сортирует
// (https://golang.org/pkg/sort/), объединяет отсортированный
// результат через _ (символ подчеркивания) в одну строку
func CombineResults(in, out chan interface{}) {
	var hashes []string
	for hash := range in {
		hashStr, ok := hash.(string)
		if !ok {
			panic(fmt.Errorf("could not convert to string: %#v", hash))
		}
		hashes = append(hashes, hashStr)
	}
	if len(hashes) == 0 {
		return
	}

	sort.Strings(hashes)

	res := strings.Builder{}
	for i := 0; i < len(hashes)-1; i++ {
		res.WriteString(hashes[i] + "_")
	}
	res.WriteString(hashes[len(hashes)-1])
	out <- res.String()
}
