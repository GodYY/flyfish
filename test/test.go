package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"unsafe"
)

var byteToString = []string{
	"\\000",
	"\\001",
	"\\002",
	"\\003",
	"\\004",
	"\\005",
	"\\006",
	"\\007",
	"\\010",
	"\\011",
	"\\012",
	"\\013",
	"\\014",
	"\\015",
	"\\016",
	"\\017",
	"\\020",
	"\\021",
	"\\022",
	"\\023",
	"\\024",
	"\\025",
	"\\026",
	"\\027",
	"\\030",
	"\\031",
	"\\032",
	"\\033",
	"\\034",
	"\\035",
	"\\036",
	"\\037",
	"\\040",
	"\\041",
	"\\042",
	"\\043",
	"\\044",
	"\\045",
	"\\046",
	"\\047",
	"\\050",
	"\\051",
	"\\052",
	"\\053",
	"\\054",
	"\\055",
	"\\056",
	"\\057",
	"\\060",
	"\\061",
	"\\062",
	"\\063",
	"\\064",
	"\\065",
	"\\066",
	"\\067",
	"\\070",
	"\\071",
	"\\072",
	"\\073",
	"\\074",
	"\\075",
	"\\076",
	"\\077",
	"\\100",
	"\\101",
	"\\102",
	"\\103",
	"\\104",
	"\\105",
	"\\106",
	"\\107",
	"\\110",
	"\\111",
	"\\112",
	"\\113",
	"\\114",
	"\\115",
	"\\116",
	"\\117",
	"\\120",
	"\\121",
	"\\122",
	"\\123",
	"\\124",
	"\\125",
	"\\126",
	"\\127",
	"\\130",
	"\\131",
	"\\132",
	"\\133",
	"\\134",
	"\\135",
	"\\136",
	"\\137",
	"\\140",
	"\\141",
	"\\142",
	"\\143",
	"\\144",
	"\\145",
	"\\146",
	"\\147",
	"\\150",
	"\\151",
	"\\152",
	"\\153",
	"\\154",
	"\\155",
	"\\156",
	"\\157",
	"\\160",
	"\\161",
	"\\162",
	"\\163",
	"\\164",
	"\\165",
	"\\166",
	"\\167",
	"\\170",
	"\\171",
	"\\172",
	"\\173",
	"\\174",
	"\\175",
	"\\176",
	"\\177",
	"\\200",
	"\\201",
	"\\202",
	"\\203",
	"\\204",
	"\\205",
	"\\206",
	"\\207",
	"\\210",
	"\\211",
	"\\212",
	"\\213",
	"\\214",
	"\\215",
	"\\216",
	"\\217",
	"\\220",
	"\\221",
	"\\222",
	"\\223",
	"\\224",
	"\\225",
	"\\226",
	"\\227",
	"\\230",
	"\\231",
	"\\232",
	"\\233",
	"\\234",
	"\\235",
	"\\236",
	"\\237",
	"\\240",
	"\\241",
	"\\242",
	"\\243",
	"\\244",
	"\\245",
	"\\246",
	"\\247",
	"\\250",
	"\\251",
	"\\252",
	"\\253",
	"\\254",
	"\\255",
	"\\256",
	"\\257",
	"\\260",
	"\\261",
	"\\262",
	"\\263",
	"\\264",
	"\\265",
	"\\266",
	"\\267",
	"\\270",
	"\\271",
	"\\272",
	"\\273",
	"\\274",
	"\\275",
	"\\276",
	"\\277",
	"\\300",
	"\\301",
	"\\302",
	"\\303",
	"\\304",
	"\\305",
	"\\306",
	"\\307",
	"\\310",
	"\\311",
	"\\312",
	"\\313",
	"\\314",
	"\\315",
	"\\316",
	"\\317",
	"\\320",
	"\\321",
	"\\322",
	"\\323",
	"\\324",
	"\\325",
	"\\326",
	"\\327",
	"\\330",
	"\\331",
	"\\332",
	"\\333",
	"\\334",
	"\\335",
	"\\336",
	"\\337",
	"\\340",
	"\\341",
	"\\342",
	"\\343",
	"\\344",
	"\\345",
	"\\346",
	"\\347",
	"\\350",
	"\\351",
	"\\352",
	"\\353",
	"\\354",
	"\\355",
	"\\356",
	"\\357",
	"\\360",
	"\\361",
	"\\362",
	"\\363",
	"\\364",
	"\\365",
	"\\366",
	"\\367",
	"\\370",
	"\\371",
	"\\372",
	"\\373",
	"\\374",
	"\\375",
	"\\376",
	"\\377",
}

var suffix = []byte{'\047', '\072', '\072', '\142', '\171', '\164', '\145', '\141'}

//'\075'::bytea
func binaryToPgsqlStr(bytes []byte) string {
	l := len(bytes)*4 + 9

	fmt.Println(l)

	out := make([]byte, l)
	out[0] = '\047'
	offset := 1
	for _, v := range bytes {
		s := byteToString[int(v)]
		fmt.Println(s)
		copy(out[offset:], s)
		offset += len(s)
	}

	copy(out[offset:], suffix)

	return *(*string)(unsafe.Pointer(&out))
}

func getFileList(dirpath string) ([]string, error) {
	var file_list []string
	dir_err := filepath.Walk(dirpath,
		func(path string, f os.FileInfo, err error) error {
			if f == nil {
				return err
			}
			if !f.IsDir() {
				file_list = append(file_list, path)
				return nil
			}

			return nil
		})
	return file_list, dir_err
}

type ByID []string

func (a ByID) Len() int      { return len(a) }
func (a ByID) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByID) Less(i, j int) bool {
	l := a[i]
	r := a[j]
	fieldl := strings.Split(l, "_")
	fieldr := strings.Split(r, "_")
	if len(fieldl) != 2 || len(fieldr) != 2 {
		panic("invaild writeBack file")
	}

	idl, err := strconv.ParseInt(strings.TrimRight(fieldl[1], ".wb"), 10, 64)
	if nil != err {
		panic("invaild writeBack file")
	}

	idr, err := strconv.ParseInt(strings.TrimRight(fieldr[1], ".wb"), 10, 64)
	if nil != err {
		panic("invaild writeBack file")
	}

	return idl < idr

}

func sortFileList(fileList []string) {
	sort.Sort(ByID(fileList))
}

func main() {

	//bytes := []byte{'\142', '\171', '\164', '\145', '\141'}

	//bytes := []byte{'\075'}

	//s := binaryToPgsqlStr(bytes)

	//fmt.Println(s, len(s))

	/*stat, err := os.Stat("writeBack")

	if nil != err && os.IsNotExist(err) {
		fmt.Println("IsNotExist")
	}

	fmt.Println(stat, err)*/

	//l, _ := getFileList("tmpWriteBackOp")

	//fmt.Println(l)

	fileList := []string{
		"writeBackFile_10.wb",
		"writeBackFile_1.wb",
		"writeBackFile_2.wb",
		"writeBackFile_3.wb",
		"writeBackFile_4.wb",
		"writeBackFile_5.wb",
		"writeBackFile_6.wb",
		"writeBackFile_7.wb",
		"writeBackFile_8.wb",
		"writeBackFile_9.wb",
	}

	sortFileList(fileList)

	for _, v := range fileList {
		fmt.Println(v)
	}

	//fmt.Println(binaryToPgsqlStr(bytes))

	//	fmt.Println(bytes)

	//s := "\\008"
	//fmt.Println(len(s))
	//for i := 0; i < 256; i++ {
	//	fmt.Printf("\\"\\\%03o\",\n", i)
	//}
}
