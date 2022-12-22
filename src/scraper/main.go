package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PuerkitoBio/goquery"
	"golang.org/x/sync/semaphore"
)

var CENTER_NAMES = []string{
	"WAC",
	"EAC",
	"MSC",
	"LIN",
	"SRC",
	"IOE",
	// "YSC" seems nobody cares, only handles 1 form type
}

var FORM_TYPES = []string{
	"CRI-89",
	"EOIR-29",
	"I-129CW",
	"I-129F",
	"I-600A",
	"I-601A",
	"I-765V",
	"I-485J",
	"I-800A",
	"I-821D",
	"I-90",
	"I-102",
	"I-129",
	"I-130",
	"I-131",
	"I-140",
	"I-212",
	"I-290B",
	"I-360",
	"I-485",
	"I-526",
	"I-539",
	"I-600",
	"I-601",
	"I-612",
	"I-730",
	"I-751",
	"I-765",
	"I-800",
	"I-817",
	"I-821",
	"I-824",
	"I-829",
	"I-865",
	"I-914",
	"I-918",
	"I-924",
	"I-929",
	"N-400",
}

var thisFY = 23
var fyStart = time.Date(2000+thisFY-1, 10, 1, 1, 1, 1, 0, time.Local)
var yearDayFyToday = int(time.Now().Sub(fyStart).Hours()/24) + 1

var FYs = []string{
	"21",
	"22",
	"23",
}

var MONTH_NAMES = []string{
	"Jan",
	"Feb",
	"Mar",
	"Apr",
	"May",
	"Jun",
	"Jul",
	"Aug",
	"Sept",
	"Oct",
	"Nov",
	"Dec",
}

type Result struct {
	Status string
	Form   string
}

type CaseState struct {
	Form    string
	Status  string
	Offset  int
	Summary string
}

type RawStorage struct {
	Index  map[Result]int
	Status map[string]int
}

var case_date_store = make(map[string]string)

var case_state_map = make(map[string]CaseState)
var case_status_store = make(map[string]int)
var case_status_index_store = make(map[Result]int)
var case_form_type_global_cache = make(map[string]string)
var case_status_index = 0
var report_freq int64 = 10000
var report_retry int64 = 0
var report_get int64 = 0

const (
	center_year_day_code_serial = iota
	center_year_code_day_serial
)

var mutex sync.Mutex
var case_date_store_mutex sync.Mutex
var case_status_store_mutex sync.Mutex
var case_form_store_mutex sync.Mutex
var epoch_day = time.Now().Unix() / 86400
var sem = semaphore.NewWeighted(900)

var start_epoch = time.Now().Unix()
var last_record = start_epoch
var client = &http.Client{
	Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
}

func get(form url.Values, retry int) Result {
	sem.Acquire(context.Background(), 1)
	res, err1 := client.PostForm("https://egov.uscis.gov/casestatus/mycasestatus.do", form)

	sem.Release(1)

	atomic.AddInt64(&report_get, 1)
	if retry > 0 {
		atomic.AddInt64(&report_retry, 1)
	}

	defer func() {
		if err1 == nil {
			res.Body.Close()
		}
	}()
	if err1 != nil {
		fmt.Println("error 1! " + err1.Error() + "\n")
		fmt.Printf("Retry %d %s\n", retry, form)
		return get(form, retry+1)
	}

	doc, err2 := goquery.NewDocumentFromReader(res.Body)
	if err2 != nil {
		fmt.Println("error 2! " + err2.Error() + "\n")
		fmt.Printf("Retry %d %s\n", retry, form)
		return get(form, retry+1)
	}

	body := doc.Find(".rows").First()
	status := body.Find("h1").Text()
	case_id := form.Get("appReceiptNum")
	bodytext := body.Text()
	for _, form := range FORM_TYPES {
		if strings.Contains(bodytext, form) {
			case_form_store_mutex.Lock()
			case_form_type_global_cache[case_id] = form
			case_form_store_mutex.Unlock()
			return Result{status, form}
		}
	}

	for _, mn := range MONTH_NAMES {
		if mon_pos := strings.Index(bodytext, mn); mon_pos >= 0 {
			if year_pos := strings.Index(bodytext, "20"); year_pos > mon_pos {
				case_date_store_mutex.Lock()
				case_date_store[case_id] = bodytext[0 : year_pos+4]
				case_date_store_mutex.Unlock()
				break
			}
		}
	}

	if status != "" {
		case_form_store_mutex.Lock()
		cachedForm, ok := case_form_type_global_cache[case_id]
		case_form_store_mutex.Unlock()
		if ok {
			return Result{status, cachedForm}
		} else {
			return Result{status, "unknown"}
		}
	} else {
		return Result{"", ""}
	}
}

func toURL(center string, two_digit_yr int, day int, code int, case_serial_numbers int, format int) url.Values {
	if format == center_year_day_code_serial {
		res := url.Values{"appReceiptNum": {fmt.Sprintf("%s%d%03d%d%04d", center, two_digit_yr, day, code, case_serial_numbers)}}
		return res
	} else if center == "IOE" {
		res := url.Values{"appReceiptNum": {fmt.Sprintf("%s09%04d%04d", center, (two_digit_yr-21)*366+day+1000, case_serial_numbers)}}
		return res
	} else {
		res := url.Values{"appReceiptNum": {fmt.Sprintf("%s%d%d%03d%04d", center, two_digit_yr, code, day, case_serial_numbers)}}
		return res
	}
}

func clawAsync(center string, two_digit_yr int, day int, code int, case_serial_numbers int, format int, c chan Result) {
	c <- claw(center, two_digit_yr, day, code, case_serial_numbers, format)
}

func claw(center string, two_digit_yr int, day int, code int, case_serial_numbers int, format int) Result {
	if center == "IOE" && format == center_year_day_code_serial {
		return Result{"", ""}
	}
	url := toURL(center, two_digit_yr, day, code, case_serial_numbers, format)
	case_id := url.Get("appReceiptNum")
	res := get(url, 0)

	if res.Status != "" {
		case_status_store_mutex.Lock()
		ind, has := case_status_index_store[res]
		if !has {
			case_status_index_store[res] = case_status_index
			ind = case_status_index
			case_status_index++
		}
		case_status_store[case_id] = ind
		if len(case_status_store) > 0 && len(case_status_store)%int(report_freq) == 0 {
			now := time.Now().Unix()
			if now != last_record {
				fmt.Printf("\t\t\tQPS for previous %d: %d\n", report_freq, report_freq/(now-last_record))
				last_record = now
			}
		}
		case_status_store_mutex.Unlock()
	}

	return res
}

func clawWithCache(center string, two_digit_yr int, day int, code int, case_serial_numbers int, format int) Result {
	url := toURL(center, two_digit_yr, day, code, case_serial_numbers, format)
	case_id := url.Get("appReceiptNum")
	case_form_store_mutex.Lock()
	cachedForm, ok := case_form_type_global_cache[case_id]
	case_form_store_mutex.Unlock()
	if ok {
		return Result{"test", cachedForm}
		//} else if two_digit_yr < 23 {
		//	return Result{"", ""}
	} else {
		return claw(center, two_digit_yr, day, code, case_serial_numbers, format)
	}
}

func getLastCaseNumber(center string, two_digit_yr int, day int, code int, format int) int {
	low := 1
	high := 1
	for (clawWithCache(center, two_digit_yr, day, code, high, format).Status != "" ||
		clawWithCache(center, two_digit_yr, day, code, high+1, format).Status != "" ||
		clawWithCache(center, two_digit_yr, day, code, high+2, format).Status != "" ||
		clawWithCache(center, two_digit_yr, day, code, high+3, format).Status != "" ||
		clawWithCache(center, two_digit_yr, day, code, high+4, format).Status != "") && high < 10000 {
		high *= 2
	}
	for low < high {
		mid := (low + high) / 2
		if clawWithCache(center, two_digit_yr, day, code, mid, format).Status != "" ||
			clawWithCache(center, two_digit_yr, day, code, mid+1, format).Status != "" ||
			clawWithCache(center, two_digit_yr, day, code, mid+2, format).Status != "" ||
			clawWithCache(center, two_digit_yr, day, code, mid+3, format).Status != "" ||
			clawWithCache(center, two_digit_yr, day, code, mid+4, format).Status != "" {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return low - 1
}

func all(center string, two_digit_yr int, day int, code int, format int, report_c chan int) {
	defer func() { report_c <- 0 }()
	if two_digit_yr >= thisFY && day > yearDayFyToday && center != "IOE" {
		return
	}
	dir, _ := os.Getwd()
	var path string
	if format == center_year_day_code_serial {
		path = fmt.Sprintf("%s/data_center_year_day_code_serial_%d.json", dir, two_digit_yr)
	} else {
		path = fmt.Sprintf("%s/data_center_year_code_day_serial_%d.json", dir, two_digit_yr)
	}

	last := getLastCaseNumber(center, two_digit_yr, day, code, format)
	fmt.Printf("loading %s total of %d at day %d of format %d\n", center, last, day, format)
	c := make(chan Result)
	for i := 1; i < last; i++ {
		go clawAsync(center, two_digit_yr, day, code, i, format, c)
	}
	counter := make(map[string]map[int64]int)
	for i := 1; i < last; i++ {
		cur := <-c
		if cur.Status == "" || cur.Form == "" {
			continue
		}

		key := fmt.Sprintf("%s|%d|%d|%d|%s|%s", center, two_digit_yr, day, code, cur.Form, cur.Status)

		if counter[key] == nil {
			counter[key] = make(map[int64]int)
		}
		counter[key][epoch_day] += 1
	}
	mutex.Lock()
	existingCounter := make(map[string]map[int64]int)
	jsonFile := readF(path)
	json.Unmarshal([]byte(jsonFile), &existingCounter)
	getMerged(existingCounter, counter)
	b, _ := json.MarshalIndent(existingCounter, "", "  ")
	writeF(path, b)

	mutex.Unlock()
	fmt.Printf("Done %s total of %d at day %d of format %d\n", center, last, day, format)
}

func readF(path string) []byte {
	f, err := os.ReadFile(path)
	for err != nil {
		fmt.Println("error read! " + err.Error() + "\n")
		f, err = os.ReadFile(path)
	}
	return f
}

func writeF(path string, content []byte) {
	err := os.WriteFile(path, content, 0666)
	for err != nil {
		fmt.Println("error write! " + err.Error() + "\n")
		err = os.WriteFile(path, content, 0666)
	}
}

func getMerged(m1, m2 map[string]map[int64]int) {
	for key, counter := range m2 {
		if m1[key] == nil {
			m1[key] = counter
		} else {
			for day, count := range counter {
				m1[key][day] = count
			}
		}
	}

	for _, counter := range m1 {
		for day := range counter {
			if epoch_day-day > 7 {
				delete(counter, day)
			}
		}
	}
}

func build_transitioning_map(delta int) {
	b_old, err1 := os.Open(fmt.Sprintf("./nocommit/%d.bytes", epoch_day-int64(delta)))
	b_new, err2 := os.Open(fmt.Sprintf("./nocommit/%d.bytes", epoch_day))
	if err1 != nil || err2 != nil {
		return
	}

	d_old := gob.NewDecoder(b_old)
	d_new := gob.NewDecoder(b_new)

	var raw_old RawStorage
	if err := d_old.Decode(&raw_old); err != nil {
		panic(err)
	}
	var raw_new RawStorage
	if err := d_new.Decode(&raw_new); err != nil {
		panic(err)
	}

	reverse_map_old := make(map[int]Result)
	for key, value := range raw_old.Index {
		reverse_map_old[value] = key
	}
	reverse_map_new := make(map[int]Result)
	for key, value := range raw_new.Index {
		reverse_map_new[value] = key
	}

	// center_year_day_code_serial|form|center|year|day|code|from|to -> count
	// center_year_code_day_serial|form|center|year|code|day|from|to -> count
	transitioning_map := make(map[string]int)
	for caseid, case_status_index_new := range raw_new.Status {
		case_status_new := reverse_map_new[case_status_index_new]
		case_status_old, ok := reverse_map_old[raw_old.Status[caseid]]
		if !ok {
			case_status_old = Result{"NEW_CASE", case_status_new.Form}
		}
		if case_status_new != case_status_old {
			var center, year, day, code, serial, count_key string

			var case_form = case_status_old.Form
			if case_form == "NEW_CASE" {
				case_form = case_status_new.Form
			}

			if caseid[0:3] == "IOE" {
				var ioeday int = 0
				fmt.Sscanf(caseid, "%3s%2s%4d%4s", &center, &year, &ioeday, &serial)
				code = "9"
				year = fmt.Sprintf("%02d", (ioeday-1000)/366+21)
				day = fmt.Sprintf("%03d", (ioeday-1000)%366)
				count_key = fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s|%s", "center_year_code_day_serial", case_form, center, year, code, day, case_status_old.Status, case_status_new.Status)
			} else if caseid[5:6] == "9" {
				fmt.Sscanf(caseid, "%3s%2s%1s%3s%4s", &center, &year, &code, &day, &serial)
				count_key = fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s|%s", "center_year_code_day_serial", case_form, center, year, code, day, case_status_old.Status, case_status_new.Status)
			} else {
				fmt.Sscanf(caseid, "%3s%2s%3s%1s%4s", &center, &year, &day, &code, &serial)
				count_key = fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s|%s", "center_year_day_code_serial", case_form, center, year, code, day, case_status_old.Status, case_status_new.Status)
			}

			if _, ok := transitioning_map[count_key]; !ok {
				transitioning_map[count_key] = 0
			}
			transitioning_map[count_key] = 1 + transitioning_map[count_key]
		}
	}
	dir, _ := os.Getwd()
	path := fmt.Sprintf("%s/transitioning_%d.json", dir, delta)
	existingTransitioningMap := make(map[int]map[string]int)
	jsonFile := readF(path)
	json.Unmarshal([]byte(jsonFile), &existingTransitioningMap)
	existingTransitioningMap[int(epoch_day)] = transitioning_map

	for day := range existingTransitioningMap {
		if int(epoch_day)-day > 7 {
			delete(existingTransitioningMap, day)
		}
	}

	b, _ := json.MarshalIndent(existingTransitioningMap, "", "  ")
	writeF(path, b)
}

func load_case_cache() {
	b, err := os.Open("./case_form_type_global_cache.bytes")
	if err != nil {
		return
	}
	d := gob.NewDecoder(b)
	if err := d.Decode(&case_form_type_global_cache); err != nil {
		panic(err)
	}

	if b_dtfile, err := os.Open(fmt.Sprintf("./nocommit/dt%d.bytes", epoch_day)); err == nil {
		d_dtfile := gob.NewDecoder(b_dtfile)
		if err := d_dtfile.Decode(&case_date_store); err != nil {
			panic(err)
		}
	}
}

func persist_case_cache() {
	buffer := new(bytes.Buffer)
	e := gob.NewEncoder(buffer)
	err := e.Encode(case_form_type_global_cache)
	if err != nil {
		panic(err)
	}

	writeF("./case_form_type_global_cache.bytes", buffer.Bytes())
}

func build_statuschange_map() {
	epoch_cur := int(epoch_day)
	tr_c, err0 := os.Open(fmt.Sprintf("./nocommit/tr_%d.bytes", epoch_cur-1))
	if err0 == nil {
		trd_c := gob.NewDecoder(tr_c)

		if err0 = trd_c.Decode(&case_state_map); err0 == nil {

		}
	}

	for ep_day := epoch_cur; ep_day <= int(epoch_day)+1; ep_day++ {
		b_c, err1 := os.Open(fmt.Sprintf("./nocommit/%d.bytes", ep_day))
		if err1 == nil {
			epoch_cur = ep_day
			reverse_map_t := make(map[int]Result)
			d_c := gob.NewDecoder(b_c)

			var raw_c RawStorage
			if err2 := d_c.Decode(&raw_c); err2 == nil {
				for key, value := range raw_c.Index {
					reverse_map_t[value] = key
				}

				for key, value := range raw_c.Status {
					res, has := reverse_map_t[value]
					curstatus, seen := case_state_map[key]

					if !has || key[3:5] != "22" && key[3:5] != "23" || res.Form != "I-765" && res.Form != "I-485" {
						continue
					}

					if has && !seen {
						case_state_map[key] = CaseState{res.Form, res.Status, ep_day, fmt.Sprintf("%d, %s", ep_day, res.Status)}
						continue
					}

					if seen && curstatus.Status != res.Status {
						case_state_map[key] = CaseState{res.Form, res.Status,
							ep_day,
							fmt.Sprintf("%d, %s, %s", ep_day-curstatus.Offset, res.Status, curstatus.Summary)}
					}
				}
			}
		}
	}

	buf_t := new(bytes.Buffer)
	e_t := gob.NewEncoder(buf_t)
	err_t := e_t.Encode(case_state_map)
	if err_t != nil {
		panic(err_t)
	}
	writeF(fmt.Sprintf("./nocommit/tr_%d.bytes", epoch_cur), buf_t.Bytes())

	for key, val := range case_state_map {
		if val.Form != "I-485" || val.Offset == 19271 || val.Status != "New Card Is Being Produced" && val.Status != "Card Was Delivered To Me By The Post Office" {
			continue
		}
		fmt.Printf("%s, %s, %s\r\n", key[0:3], key, val.Summary)
	}
}

func main() {
	debug.SetMaxThreads(20000)

	if len(os.Args) > 1 {
		CENTER_NAMES = strings.Split(os.Args[1], ",")
	}

	if len(os.Args) > 2 {
		FYs = strings.Split(os.Args[2], ",")
	}

	b_cur, err1 := os.Open(fmt.Sprintf("./nocommit/%d.bytes", epoch_day))
	if err1 == nil {
		d_cur := gob.NewDecoder(b_cur)

		var raw_cur RawStorage
		if err2 := d_cur.Decode(&raw_cur); err2 == nil {
			case_status_index_store = raw_cur.Index
			case_status_store = raw_cur.Status
			for _, value := range case_status_index_store {
				if value > case_status_index {
					case_status_index = value
				}
			}
			case_status_index++
		}
	}

	load_case_cache()

	for _, fy := range FYs {
		for _, name := range CENTER_NAMES {
			report_c_center_year_day_code_serial := make(chan int)
			report_c_center_year_code_day_serial := make(chan int)
			for day := 0; day <= 365; day++ {
				ify, _ := strconv.Atoi(fy)
				go all(name, ify, day, 5, center_year_day_code_serial, report_c_center_year_day_code_serial)
				go all(name, ify, day, 9, center_year_code_day_serial, report_c_center_year_code_day_serial)
			}
			for i := 0; i <= 365; i++ {
				<-report_c_center_year_day_code_serial
				<-report_c_center_year_code_day_serial
			}
		}
	}

	fmt.Printf("total get: %d, total retry: %d", report_get, report_retry)

	buffer := new(bytes.Buffer)
	e := gob.NewEncoder(buffer)
	err := e.Encode(RawStorage{case_status_index_store, case_status_store})
	if err != nil {
		panic(err)
	}
	writeF(fmt.Sprintf("./nocommit/%d.bytes", epoch_day), buffer.Bytes())

	b_dtfile := new(bytes.Buffer)
	e_dtfile := gob.NewEncoder(b_dtfile)
	err_dtfile := e_dtfile.Encode(case_date_store)
	if err_dtfile != nil {
		panic(err_dtfile)
	}
	writeF(fmt.Sprintf("./nocommit/dt%d.bytes", epoch_day), b_dtfile.Bytes())

	build_transitioning_map(1)
	build_transitioning_map(7)
	persist_case_cache()
}
