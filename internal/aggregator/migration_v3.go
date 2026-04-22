package aggregator

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/VKCOM/statshouse/internal/chutil"
	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/metajournal"
	"github.com/VKCOM/statshouse/internal/vkgo/kittenhouseclient/rowbinary"
)

var ABSENT_METRIC_IDS = []int32{168, 169, 170, 281, 282, 283, 284, 285, 286, 288, 289, 290, 295, 296, 299, 300, 302, 303, 304, 305, 306, 308, 309, 312, 313, 316, 317, 323, 325, 334, 342, 344, 350, 351, 352, 355, 356, 357, 358, 359, 360, 362, 367, 416, 427, 437, 439, 440, 464, 468, 475, 514, 517, 521, 529, 536, 539, 540, 541, 551, 558, 614, 625, 631, 683, 730, 763, 767, 779, 834, 898, 906, 980, 1027, 1036, 1055, 1155, 1220, 1234, 1247, 1253, 1311, 1321, 1407, 1457, 1459, 1462, 1488, 1512, 1533, 1561, 1603, 1656, 1659, 1672, 1682, 1727, 1744, 1827, 1829, 1836, 1861, 1913, 1974, 1979, 2039, 2040, 2123, 2135, 2140, 2181, 2182, 2215, 2216, 2232, 2377, 2389, 2396, 2399, 2400, 2404, 2436, 2461, 2597, 2598, 2627, 2628, 2747, 2885, 2916, 3049, 3253, 3430, 3442, 4614, 4618, 4662, 4687, 4752, 4791, 4810, 4831, 4974, 4984, 5148, 5476, 5577, 5714, 5794, 5806, 5834, 5869, 5883, 6034, 6064, 6321, 6473, 6489, 6521, 6544, 6548, 6571, 6657, 6675, 6783, 6924, 6947, 7065, 7248, 7281, 7378, 7410, 7424, 7525, 7531, 7581, 7594, 7683, 7715, 7869, 7879, 7923, 8092, 8146, 8193, 8255, 8258, 8323, 8357, 8442, 8476, 8503, 8528, 8547, 8586, 8589, 8594, 8624, 8681, 8697, 8717, 8733, 8765, 8778, 8787, 8840, 8945, 8975, 8978, 9113, 9146, 9186, 9223, 9265, 9296, 9326, 9411, 9425, 9472, 9595, 9620, 9683, 9832, 11009, 16232, 17604, 17610, 17625, 17726, 17886, 20881, 24332, 24336, 24339, 24794, 24798, 24834, 24850, 24909, 24919, 24925, 24963, 24968, 25055, 25074, 25216, 25313, 26543, 29360, 29361, 29363, 29417, 29492, 31007, 31133, 31134, 31290, 32008, 32081, 32164, 32326, 32515, 32548, 32549, 32590, 57420, 71413, 89963, 94548, 96776, 104139, 122909, 138568, 231681, 232193, 232449, 232705, 233985, 234753, 236545, 237034, 237057, 240129, 244481, 249707, 288979, 305153, 341249, 346625, 349441, 350977, 388652, 405249, 458752, 524288, 532847, 590266, 596650, 635665, 636424, 653579, 675169, 698648, 707456, 713031, 740131, 763769, 786432, 796403, 834015, 854386, 867919, 869227, 885495, 889918, 906508, 972806, 1002039, 1003265, 1028297, 1031771, 1059954, 1069098, 1091178, 1101492, 1118206, 1158847, 1158876, 1172481, 1185716, 1190690, 1191141, 1228289, 1237996, 1245624, 1262790, 1318541, 1320662, 1361849, 1419331, 1426495, 1469504, 1542921, 1572864, 1617024, 1623560, 1703936, 1704057, 1704286, 1769472, 1817438, 1952701, 2008050, 2018704, 2075773, 2082944, 2127733, 2147581, 2159340, 2184218, 2217846, 2284640, 2296375, 2327517, 2404287, 2752512, 3145728, 4763955, 5046272, 6225920, 6253479, 6253494, 6253509, 6253513, 6253539, 6253659, 6253674, 6253678, 6253689, 6253708, 6619136, 7186010, 8072705, 8912896, 10415617, 11687937, 12280321, 12332801, 15195905, 15205633, 16777216, 16777225, 16781796, 16794995, 16797844, 16802341, 16802446, 16803443, 16803657, 16826541, 16827575, 16828337, 16835149, 16838775, 16842513, 16845130, 16848847, 16854788, 16864009, 16873904, 16877559, 16877580, 16879140, 16901192, 16903615, 16917870, 16920366, 16940008, 16954414, 17004079, 17012298, 17020366, 17083327, 17107541, 18612224, 21561344, 21744641, 22996764, 23829711, 23993596, 24239150, 24239152, 24239245, 24265690, 24265692, 30723585, 39868913, 47054848, 47120384, 50024982, 50868040, 57344952, 59310080, 59375616, 59441152, 59572224, 59768832, 59899904, 59965440, 60096512, 60227584, 60555264, 61603840, 63045632, 67108875, 67175100, 67175438, 75871745, 78053376, 78118912, 81538305, 82837504, 82844475, 87495104, 88604672, 88735744, 88736094, 88997888, 89456640, 94183369, 95539520, 103743488, 110962020, 111149056, 134283343, 134283516, 136011628, 140015507, 140283767, 140283768, 140443907, 141057900, 146361447, 147414892, 151061731, 164364288, 167772171, 173539328, 184615021, 185336064, 190803065, 191299843, 192806912, 195952640, 197839915, 198836224, 199098368, 200081408, 201392569, 203293042, 204800000, 207580281, 229310464, 229965824, 230424576, 230621184, 230752256, 231800832, 234946560, 251724353, 256835584, 262668288, 268370176, 268501490, 269156352, 273088512, 285212683, 296026112, 311885824, 318833455, 358575244, 362218494, 385942015, 408906892, 419496469, 425684089, 427622659, 436207628, 453050621, 454688768, 463339520, 476015756, 503382635, 505741312, 517537792, 520159232, 520160043, 536870986, 536937092, 543124601, 562888704, 576258048, 576679033, 576679052, 577110016, 587269222, 593456268, 604045318, 604046168, 606077185, 607911936, 607977472, 608043008, 608108544, 608174080, 608239616, 608829440, 614662144, 623837443, 654376994, 654377443, 660825458, 661417057, 661679988, 671154296, 677342348, 683606016, 687865979, 693984044, 712769536, 713424896, 738263994, 738264292, 740763436, 740766261, 740767285, 740767538, 744315692, 755040642, 758776066, 762315102, 804847872, 805373121, 808465969, 808990769, 812384256, 821952512, 822149161, 822149430, 822150045, 825569580, 838860805, 838860811, 841744384, 841809920, 842072064, 842151729, 842413364, 845114489, 846790986, 858652672, 858718464, 859190579, 859387192, 863961088, 872481611, 875638833, 875706674, 875968561, 876032562, 883228672, 892350777, 892352561, 892413993, 892940849, 895877377, 905969717, 909194804, 909260593, 909719601, 936247296, 942683185, 942946609, 945777804, 946012160, 956366850, 975503360, 984060417, 996109452, 997982208, 1024655360, 1031995392, 1031995509, 1051459584, 1069895681, 1071316992, 1107362672, 1123483648, 1123549184, 1123614720, 1123680256, 1123745792, 1123811328, 1123876864, 1123942400, 1124007936, 1124073472, 1124073480, 1124139008, 1124270080, 1124335616, 1124401152, 1124466688, 1124532224, 1134428160, 1140916455, 1157693590, 1163881593, 1168638208, 1174471037, 1176783361, 1180658828, 1191182347, 1192921345, 1207959563, 1208025752, 1224736890, 1241580216, 1247332865, 1247739137, 1258357881, 1272315904, 1291911845, 1304821760, 1308622970, 1313603841, 1313669120, 1324744960, 1325465617, 1325465846, 1342177333, 1342177402, 1359020235, 1359021159, 1375731723, 1375797248, 1380188420, 1384710400, 1385693444, 1392509050, 1415945729, 1417968385, 1418008065, 1419247105, 1420317441, 1423572992, 1424700929, 1426130223, 1443915009, 1445986304, 1450262529, 1451622400, 1455343105, 1455349505, 1457989889, 1458945537, 1459216897, 1459838209, 1461716225, 1465804545, 1473052672, 1473511424, 1473708293, 1476526080, 1489633280, 1490293761, 1491675393, 1494679552, 1494745088, 1495139841, 1495139842, 1498733057, 1498733059, 1500512256, 1505354753, 1515780097, 1525081601, 1526792564, 1526792570, 1529529857, 1529964801, 1537219585, 1541372161, 1541471233, 1541870593, 1541870595, 1541999361, 1546481665, 1550188804, 1552038145, 1552548096, 1552945921, 1552955905, 1558708224, 1562378496, 1562849331, 1563072257, 1563467265, 1563467777, 1563468289, 1563468545, 1563468801, 1563469057, 1563469313, 1563470337, 1563470849, 1563471361, 1563471873, 1563473153, 1563474177, 1563474433, 1563474689, 1563580673, 1563580929, 1563588865, 1563589121, 1563589377, 1563589633, 1567948800, 1572834305, 1576949249, 1580306945, 1584499457, 1586217985, 1587935489, 1587935745, 1587936257, 1587936769, 1587937025, 1588389633, 1593638912, 1593835573, 1596705281, 1596737281, 1600872449, 1600872705, 1600873473, 1600874241, 1600874753, 1600875009, 1606090752, 1610587649, 1610587905, 1610612789, 1610678436, 1610678498, 1610678839, 1619001600, 1622962945, 1627390005, 1635017059, 1637728001, 1637728257, 1644167221, 1661010021, 1666415361, 1668246626, 1682630401, 1687698177, 1688666112, 1689557761, 1689700865, 1694565359, 1699873026, 1701012850, 1701079414, 1701667175, 1701669236, 1704853504, 1710775297, 1710780161, 1712906497, 1713293315, 1715691372, 1717551363, 1717551619, 1721404929, 1729869313, 1732270849, 1734306937, 1734306997, 1734367732, 1735289190, 1737287425, 1740268545, 1747761665, 1751889921, 1752392040, 1753644033, 1753656065, 1753656321, 1753656577, 1756101377, 1756101633, 1756101889, 1756102145, 1756102147, 1756102401, 1756102657, 1756103425, 1756103937, 1756104193, 1756104449, 1756104705, 1756374785, 1761613569, 1761613825, 1761614081, 1761614337, 1761614593, 1761615105, 1761615361, 1761616385, 1761616641, 1761617153, 1761618177, 1761618689, 1761619457, 1761619969, 1765715713, 1765867779, 1778450968, 1790043649, 1790043905, 1795227956, 1801453910, 1801519446, 1814780161, 1815367169, 1819047270, 1819177217, 1824304129, 1826158593, 1828782222, 1834304513, 1836410996, 1845559463, 1861957377, 1862032897, 1862033665, 1862270986, 1869107305, 1871082241, 1871082497, 1899954176, 1911685120, 1919184449, 1928353281, 1929445513, 1951705857, 1952540771, 1953259891, 1953458288, 1953722224, 1953890304, 1963000531, 1965156097, 1967888897, 1970235393, 1970995457, 1971451137, 1974230017, 1975438081, 1975515393, 1980933377, 1981481216, 1996555011, 1997390849, 2003792482, 2007433473, 2013331469, 2021785600, 2022244610, 2022727532, 2030109868, 2034431745, 2034434305, 2047237996, 2052248321, 2057437441, 2063597621, 2080440343, 2080441226, 2087845888, 2089091072, 2105955073, 2105955329, 2105955841, 2105973505, 2105973761, 2109319681, 2113994765, 2138716929, 2146396417, 2147456769, 2147483560}

const V3_PK_COLUMNS_STR = "index_type, metric, pre_tag, pre_stag, time, tag0, stag0, tag1, stag1, tag2, stag2, tag3, stag3, tag4, stag4, tag5, stag5, tag6, stag6, tag7, stag7, tag8, stag8, tag9, stag9, tag10, stag10, tag11, stag11, tag12, stag12, tag13, stag13, tag14, stag14, tag15, stag15, tag16, stag16, tag17, stag17, tag18, stag18, tag19, stag19, tag20, stag20, tag21, stag21, tag22, stag22, tag23, stag23, tag24, stag24, tag25, stag25, tag26, stag26, tag27, stag27, tag28, stag28, tag29, stag29, tag30, stag30, tag31, stag31, tag32, stag32, tag33, stag33, tag34, stag34, tag35, stag35, tag36, stag36, tag37, stag37, tag38, stag38, tag39, stag39, tag40, stag40, tag41, stag41, tag42, stag42, tag43, stag43, tag44, stag44, tag45, stag45, tag46, stag46, tag47, stag47"
const V3_VALUE_COLUMNS_STR = "count, min, max, max_count, sum, sumsquare, min_host, max_host, max_count_host, percentiles, uniq_state"

var SKIP_METRICS = []int32{
	144155918, // members_cache 		- large onecloud metric (broken, unsupported)
	144157406, // one_cloud_cloud_stat 	- large onecloud metric (broken, unsupported)
}

type MigrationConfigV3 struct {
	SourceTableName             string        // Source table name (default: "statshouse_value_dist_1h")
	TargetTableName             string        // Destination table name (default: "statshouse_v3_1h")
	StateTableName              string        // Migration state table name
	LogsTableName               string        // Migration logs table name
	StepDuration                time.Duration // Time step for migration (default: time.Hour)
	TotalShards                 int           // Total number of shards (default: 18)
	ShardsForShardingById       int           // Number of shards for an old sharding strategy (metricId % num_shards)
	Format                      string
	ReplacementMappingsFileName string
	ReplacementMappingsDirPath  string
}

type MigrationV3Data struct {
	replacementMappings map[int32]struct{}
	mappingsStorage     *metajournal.MappingsStorage
	metricMetaLoader    *metajournal.MetricMetaLoader
	isRawTagOfMetric    map[int32][]bool
}

// v3Row models the subset of columns needed from the V1 schema.
type v3Row struct {
	index_type     uint8
	metric         int32
	pre_tag        uint32
	pre_stag       string
	time           uint32
	tags           [48]int32
	stags          [48]string
	count          float64
	min            float64
	max            float64
	max_count      float64
	sum            float64
	sumsquare      float64
	min_host       data_model.ArgMinStringFloat32
	max_host       data_model.ArgMaxStringFloat32
	max_count_host data_model.ArgMaxStringFloat32
	percentiles    data_model.ChDigest
	uniq_state     data_model.ChUnique
}

func NewDefaultMigrationConfigV3(cacheDir string) *MigrationConfigV3 {
	return &MigrationConfigV3{
		SourceTableName:             "statshouse_v3_1h",
		TargetTableName:             "statshouse_v6_1h",
		StateTableName:              "statshouse_migration_state",
		LogsTableName:               "statshouse_migration_logs",
		StepDuration:                time.Hour,
		TotalShards:                 18,
		ShardsForShardingById:       16,
		Format:                      "RowBinary",
		ReplacementMappingsFileName: "replacement_mappings.csv",
		ReplacementMappingsDirPath:  cacheDir,
	}
}

func MakeMigrationV3Data(mappingStorage *metajournal.MappingsStorage) *MigrationV3Data {
	return &MigrationV3Data{
		replacementMappings: make(map[int32]struct{}, 3_000),
		mappingsStorage:     mappingStorage,
		isRawTagOfMetric:    make(map[int32][]bool),
	}
}

func (a *Aggregator) goMigrateV3(cancelCtx context.Context) {
	if a.replicaKey != 1 {
		log.Printf("[migration_v3] Skipping migration: replica key is %d, expected 1", a.replicaKey)
		return // Only one replica should run migration per shard
	}
	// when shards are added we don't want to automatically migrate data into them
	if a.shardKey > int32(a.migrationConfigV3.TotalShards) {
		log.Printf("[migration_v3] Skipping migration: shard key is %d, expected less than %d", a.shardKey, a.migrationConfig.TotalShards)
		return
	}

	httpClient := makeHTTPClient()

	err := a.loadMigrationData()
	if err != nil {
		log.Printf("[migration_v3] Skipping migration: failed to load migration data: %s", err)
		return
	}

	log.Printf("[migration_v3] Starting migration routine for shard %d", a.shardKey)
	for {
		// Check if we should continue migrating
		select {
		case <-cancelCtx.Done():
			log.Println("[migration_v3] Exiting migration routine (context cancelled)")
			return
		default:
		}

		//Check if migration is enabled (time range must be configured)
		a.configMu.RLock()
		_, isShardDisabled := a.configR.MigrationV3DisabledShards[a.shardKey]
		isTimeRangeEmpty := a.configR.MigrationTimeRange == ""
		delaySec := a.configR.MigrationDelaySec
		a.configMu.RUnlock()

		if isTimeRangeEmpty || isShardDisabled {
			if isTimeRangeEmpty {
				log.Println("[migration_v3] Migration disabled: no time range configured")
			}
			if isShardDisabled {
				log.Printf("[migration_v3] Migration disabled: shard %d disabled", a.shardKey)
			}
			time.Sleep(time.Duration(delaySec) * time.Second)
			continue
		}

		a.migrationMu.Lock()
		insertTimeEWMA := a.insertTimeEWMA
		lastErrorTs := a.lastErrorTs
		a.migrationMu.Unlock()

		// Check system load before proceeding
		nowUnix := uint32(time.Now().Unix())
		if lastErrorTs != 0 && nowUnix >= lastErrorTs && nowUnix-lastErrorTs < noErrorsWindow {
			log.Printf("[migration_v3] Skipping: last error was %d seconds ago", nowUnix-lastErrorTs)
			time.Sleep(time.Duration(noErrorsWindow-(nowUnix-lastErrorTs)) * time.Second)
			continue
		}
		if insertTimeEWMA > maxInsertTime {
			log.Printf("[migration_v3] Skipping: EWMA insert time is too high (%.2fs)", insertTimeEWMA)
			a.configMu.RLock()
			delaySec := a.configR.MigrationDelaySec
			a.configMu.RUnlock()
			time.Sleep(time.Duration(delaySec) * time.Second)
			continue
		}

		// Find next timestamp to migrate
		nextTs, err := a.findNextTimestampToMigrateV3(httpClient)

		if err != nil {
			log.Printf("[migration_v3] Error finding next timestamp: %v", err)
			a.configMu.RLock()
			delaySec := a.configR.MigrationDelaySec
			a.configMu.RUnlock()
			time.Sleep(time.Duration(delaySec) * time.Second)
			continue
		}

		if nextTs.IsZero() {
			log.Println("[migration_v3] No more timestamps to migrate, migration complete!")
			return
		}

		log.Printf("[migration_v3] Processing timestamp: %s", nextTs.Format("2006-01-02 15:04:05"))

		// Perform migration for this timestamp
		sourceRowsNum, targetRowsNum, err := a.migrateTimestampWithRetryV3(httpClient, nextTs)
		if err != nil {
			log.Printf("[migration_v3] Failed to migrate timestamp %s: %v", nextTs.Format("2006-01-02 15:04:05"), err)
			a.configMu.RLock()
			delaySec := a.configR.MigrationDelaySec
			a.configMu.RUnlock()
			time.Sleep(time.Duration(delaySec) * time.Second)
			continue
		}

		log.Printf("[migration_v3] Successfully migrated timestamp %s (source_rows=%d, target_rows=%d)", nextTs.Format("2006-01-02 15:04:05"), sourceRowsNum, targetRowsNum)

		// Small delay before processing next timestamp
		time.Sleep(time.Second)
	}
}

func (a *Aggregator) loadMigrationData() error {
	maxMapping, err := a.loadReplacementMappingsFromFile()
	if err != nil {
		return err
	}

	err = a.loadMappingsStorageWithReverse(maxMapping)
	if err != nil {
		return err
	}

	return nil
}

func (a *Aggregator) isRelevantMetric(metricID int32) bool {
	// assumes the metric is relevant unless possible to determine that it's not

	if slices.Contains(SKIP_METRICS, metricID) {
		return false
	}

	if metricID < 0 {
		// builtin metrics are relevant if they're supported now (present in `format.BuiltinMetrics`)
		// otherwise we skip them
		m := format.BuiltinMetrics[metricID]
		return m != nil
	}

	if a.metricStorage == nil {
		return true
	}
	metric := a.metricStorage.GetMetaMetric(metricID)
	if metric == nil {
		if slices.Contains(ABSENT_METRIC_IDS, metricID) {
			// some metrics are present in ch, but absent in metadata, so we skip those
			return false
		}
		return true
	}

	shardNum := metric.Shard(a.migrationConfigV3.ShardsForShardingById)
	if shardNum == -1 {
		return true
	}

	if int32(shardNum+1) == a.shardKey {
		return true
	}
	// only if we know metric, strategy and shard
	return false
}

func (a *Aggregator) loadReplacementMappingsFromFile() (maxMapping int32, err error) {
	filePath := filepath.Join(a.migrationConfigV3.ReplacementMappingsDirPath, a.migrationConfigV3.ReplacementMappingsFileName)
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0)
	if err != nil {
		return -1, err
	}
	defer file.Close()

	in := bufio.NewReader(file)

	log.Printf("reading replacement mappings set")
	for {
		var x int32
		_, err = fmt.Fscanln(in, &x)
		if err == io.EOF {
			break
		}
		if err != nil {
			return -1, err
		}
		maxMapping = max(maxMapping, x)
		a.migrationV3Data.replacementMappings[x] = struct{}{}
	}
	log.Printf("[migration_v3] Mappings for replacement loaded from file %s", a.migrationConfigV3.ReplacementMappingsFileName)
	return maxMapping, nil
}

func (a *Aggregator) loadMappingsStorageWithReverse(maxVer int32) error {

	a.migrationV3Data.mappingsStorage.StartPeriodicSaving()
	log.Printf("[migration_v3] Starting mapping update loop")
	a.migrationV3Data.mappingsStorage.UpdateMappingsUntilVersion(maxVer, format.TagValueIDComponentAggregator, a.migrationV3Data.metricMetaLoader.GetNewMappings)
	log.Printf("[migration_v3] Successfully loaded mappings for max ver: %d", maxVer)
	return nil
}

func (a *Aggregator) migrateTimestampWithRetryV3(httpClient *http.Client, ts time.Time) (sourceRowsNum, targetRowsNum uint64, err error) {
	var lastErr error
	started := time.Now()
	sourceRowsNum, sourceCountErr := a.countSourceRowsForTs(httpClient, ts)
	if sourceCountErr != nil {
		log.Printf("[migration_v3] Warning: failed to count source rows: %v", sourceCountErr)
		sourceRowsNum = 0
	}
	for attempt := 0; attempt < maxRetryAttempts; attempt++ {
		if attempt > 0 {
			log.Printf("[migration_v3] Retry attempt %d for timestamp %s", attempt+1, ts.Format("2006-01-02 15:04:05"))
			time.Sleep(retryDelay * time.Duration(math.Pow(2, float64(attempt))))
		}
		a.updateMigrationStateV3(httpClient, ts, sourceRowsNum, 0, uint32(attempt), started, nil, migrationSourceV3)

		err = a.migrateSingleStepV3(ts, httpClient)

		if err == nil {
			targetRowsNum, countErr := a.countTargetRows(httpClient, ts)
			if countErr != nil {
				log.Printf("[migration_v3] Warning: failed to count target rows: %v", countErr)
				targetRowsNum = 0
			}
			now := time.Now()
			a.updateMigrationStateV3(httpClient, ts, sourceRowsNum, targetRowsNum, uint32(attempt), started, &now, migrationSourceV3)

			// Write migration log metric
			migrationTags := []int32{0, int32(ts.Unix()), format.TagValueIDMigrationSourceV3}
			a.sh2.AddValueCounterHost(uint32(now.Unix()), format.BuiltinMetricMetaMigrationLog, migrationTags, float64(targetRowsNum), 1, a.aggregatorHostTag)

			return sourceRowsNum, targetRowsNum, nil
		}
		lastErr = err
		log.Printf("[migration_v3] Attempt %d failed for timestamp %s: %v", attempt+1, ts.Format("2006-01-02 15:04:05"), err)
		logQuery := fmt.Sprintf(`
		INSERT INTO %s
		(timestamp, shard_key, ts, retry, message, source)
		VALUES ('%s', %d, toDateTime(%d), %d, '%s', '%s')`, a.migrationConfigV3.LogsTableName, time.Now().Format("2006-01-02 15:04:05"), a.shardKey, ts.Unix(), attempt+1, err.Error(), migrationSourceV3)
		logReq := &chutil.ClickHouseHttpRequest{HttpClient: httpClient, Addr: a.config.KHAddr, User: a.config.KHUser, Password: a.config.KHPassword, Query: logQuery}
		if logResp, logErr := logReq.Execute(context.Background()); logErr == nil {
			logResp.Close()
		}
	}
	a.updateMigrationStateV3(httpClient, ts, sourceRowsNum, 0, uint32(maxRetryAttempts), started, nil, migrationSourceV3)
	return 0, 0, fmt.Errorf("migration failed after %d attempts, last error: %w", maxRetryAttempts, lastErr)
}

func (a *Aggregator) countTargetRows(httpClient *http.Client, ts time.Time) (uint64, error) {
	countQuery := fmt.Sprintf(`
		SELECT count() as cnt
		FROM %s
		WHERE time = toDateTime(%d)`,
		a.migrationConfigV3.TargetTableName, ts.Unix())

	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       a.config.KHAddr,
		User:       a.config.KHUser,
		Password:   a.config.KHPassword,
		Query:      countQuery,
	}
	resp, err := req.Execute(context.Background())
	if err != nil {
		return 0, fmt.Errorf("failed to count target rows: %w", err)
	}
	defer resp.Close()

	var count uint64
	if _, err := fmt.Fscanf(resp, "%d", &count); err != nil {
		return 0, fmt.Errorf("failed to parse target row count: %w", err)
	}

	return count, nil
}

func (a *Aggregator) updateMigrationStateV3(httpClient *http.Client, ts time.Time, sourceRows, targetRows uint64, retryCount uint32, started time.Time, ended *time.Time, source string) error {
	endedStr := "NULL"
	if ended != nil {
		endedStr = fmt.Sprintf("'%s'", ended.Format("2006-01-02 15:04:05"))
	}

	updateQuery := fmt.Sprintf(`
		INSERT INTO %s
		(shard_key, ts, started, ended, v3_rows, source_rows, retry, source)
		VALUES (%d, toDateTime(%d), '%s', %s, %d, %d, %d, '%s')`,
		a.migrationConfigV3.StateTableName, a.shardKey, ts.Unix(), started.Format("2006-01-02 15:04:05"),
		endedStr, targetRows, sourceRows, retryCount, source)

	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       a.config.KHAddr,
		User:       a.config.KHUser,
		Password:   a.config.KHPassword,
		Query:      updateQuery,
	}
	resp, err := req.Execute(context.Background())
	if err != nil {
		return fmt.Errorf("failed to update migration state: %w", err)
	}
	resp.Close()

	return nil
}

func (a *Aggregator) countSourceRowsForTs(httpClient *http.Client, ts time.Time) (uint64, error) {
	countQuery := fmt.Sprintf(`
		SELECT count() as cnt
		FROM %s
		WHERE time = toDateTime(%d)`,
		a.migrationConfigV3.SourceTableName, ts.Unix())

	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       a.config.KHAddr,
		User:       a.config.KHUser,
		Password:   a.config.KHPassword,
		Query:      countQuery,
	}
	resp, err := req.Execute(context.Background())
	if err != nil {
		return 0, fmt.Errorf("failed to count source rows: %w", err)
	}
	defer resp.Close()

	var count uint64
	if _, err := fmt.Fscanf(resp, "%d", &count); err != nil {
		return 0, fmt.Errorf("failed to parse source row count: %w", err)
	}

	return count, nil
}

func (a *Aggregator) findNextTimestampToMigrateV3(httpClient *http.Client) (time.Time, error) {
	a.configMu.RLock()
	startTs, endTs := a.configR.ParseMigrationTimeRange(a.configR.MigrationTimeRange)
	a.configMu.RUnlock()

	// If no time range configured, migration is disabled
	if startTs == 0 && endTs == 0 {
		return time.Time{}, nil
	}

	log.Printf("[migration_v3] Searching for next timestamp to migrate in range: %d (start) to %d (end)", startTs, endTs)

	latestMigratedQuery := fmt.Sprintf(`
		SELECT toUnixTimestamp(ts) as ts_unix
		FROM %s
		WHERE shard_key = %d AND source = '%s' AND ended IS NOT NULL AND ts_unix <= %d AND ts_unix >= %d
		ORDER BY ts ASC
		LIMIT 1`, a.migrationConfigV3.StateTableName, a.shardKey, migrationSourceV3, startTs, endTs)

	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       a.config.KHAddr,
		User:       a.config.KHUser,
		Password:   a.config.KHPassword,
		Query:      latestMigratedQuery,
	}
	resp, err := req.Execute(context.Background())
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to find latest migrated timestamp: %w", err)
	}
	defer resp.Close()

	// Read the entire response to check if there are any results
	scanner := bufio.NewScanner(resp)
	var latestTs int64
	foundResult := false
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue // Skip empty lines
		}
		if _, err := fmt.Sscanf(line, "%d", &latestTs); err != nil {
			return time.Time{}, fmt.Errorf("failed to parse latest migrated timestamp from line '%s': %w", line, err)
		}
		foundResult = true
		break // We only need the first (and should be only) result
	}
	if err := scanner.Err(); err != nil {
		return time.Time{}, fmt.Errorf("error reading response: %w", err)
	}

	nextTs := time.Unix(int64(startTs), 0).Truncate(a.migrationConfigV3.StepDuration)
	if foundResult {
		// Found latest migrated timestamp - start from the previous timestamp before it (backward migration)
		latestTime := time.Unix(latestTs, 0).Truncate(a.migrationConfigV3.StepDuration)
		nextTs = latestTime.Add(-a.migrationConfigV3.StepDuration)
	}
	log.Printf("[migration_v3] Next timestamp to migrate: %s", nextTs.Format("2006-01-02 15:04:05"))

	// Check if we've reached the start of the migration range (migration complete)
	endTime := time.Unix(int64(endTs), 0)
	if nextTs.Before(endTime) {
		log.Printf("[migration_v3] Next timestamp %s is before or at migration end time %s, migration complete",
			nextTs.Format("2006-01-02 15:04:05"), endTime.Format("2006-01-02 15:04:05"))
		return time.Time{}, nil
	}

	return nextTs, nil
}

func (a *Aggregator) migrateSingleStepV3(ts time.Time, httpClient *http.Client) error {
	selectQuery := fmt.Sprintf(`
		SELECT %s, %s
		FROM %s
		WHERE time = toDateTime(%d)`,
		V3_PK_COLUMNS_STR, V3_VALUE_COLUMNS_STR, a.migrationConfigV3.SourceTableName, uint32(ts.Unix()))

	v3DataResp, err := a.executeV3Query(selectQuery, true, nil, httpClient)

	if err != nil {
		return fmt.Errorf("[migration_v3] failed to execute select query: %w", err)
	}
	defer v3DataResp.Close()

	log.Printf("[migration_v3] Successfully retrieved shard data, converting and inserting...")

	insertQuery := fmt.Sprintf(`INSERT INTO %s (%s, %s) FORMAT %s`,
		a.migrationConfigV3.TargetTableName,
		V3_PK_COLUMNS_STR,
		V3_VALUE_COLUMNS_STR,
		a.migrationConfigV3.Format,
	)

	pipeReader, pipeWriter := io.Pipe()
	reader := bufio.NewReaderSize(v3DataResp, 8192)

	var rowsConverted int
	var tagsStringified int
	var conversionErr error
	go func() {
		defer pipeWriter.Close()
		rowsConverted, tagsStringified, conversionErr = a.convertV3Response(reader, pipeWriter, ts)
		if conversionErr != nil {
			log.Printf("[migration_v3] Error during conversion: %v", conversionErr)
			pipeWriter.CloseWithError(conversionErr)
		}
	}()

	bodyBytes, err := io.ReadAll(pipeReader)
	if err != nil {
		return fmt.Errorf("[migration_v3] failed to read body: %w", err)
	}

	log.Printf("[migration_v3] Converted %d rows, stringified %d tags, body size: %d bytes", rowsConverted, tagsStringified, len(bodyBytes))

	resp, err := a.executeV3Query(insertQuery, false, bodyBytes, httpClient)

	if err != nil {
		return fmt.Errorf("[migration_v3] failed to execute insert query: %w", err)
	}

	defer resp.Close()
	log.Printf("[migration_v3] finished done for ts %d", uint32(ts.Unix()))
	return nil

}

func parseV3RowOptimized(reader *bufio.Reader, scratch []byte, row *v3Row) ([]byte, error) {
	var err error
	if scratch, err = ReadUInt8(reader, scratch, &row.index_type); err != nil {
		return scratch, err
	}
	if scratch, err = ReadInt32(reader, scratch, &row.metric); err != nil {
		return scratch, err
	}
	if scratch, err = ReadUInt32(reader, scratch, &row.pre_tag); err != nil {
		return scratch, err
	}
	if scratch, err = readString(reader, scratch, &row.pre_stag); err != nil {
		return scratch, err
	}
	if scratch, err = ReadUInt32(reader, scratch, &row.time); err != nil {
		return scratch, err
	}

	for i := 0; i < 48; i++ {
		if scratch, err = ReadInt32(reader, scratch, &row.tags[i]); err != nil {
			return scratch, err
		}
		if scratch, err = readString(reader, scratch, &row.stags[i]); err != nil {
			return scratch, err
		}
	}

	if scratch, err = ReadFloat64(reader, scratch, &row.count); err != nil {
		return scratch, err
	}
	if scratch, err = ReadFloat64(reader, scratch, &row.min); err != nil {
		return scratch, err
	}
	if scratch, err = ReadFloat64(reader, scratch, &row.max); err != nil {
		return scratch, err
	}
	if scratch, err = ReadFloat64(reader, scratch, &row.max_count); err != nil {
		return scratch, err
	}
	if scratch, err = ReadFloat64(reader, scratch, &row.sum); err != nil {
		return scratch, err
	}
	if scratch, err = ReadFloat64(reader, scratch, &row.sumsquare); err != nil {
		return scratch, err
	}

	if scratch, err = row.min_host.ReadFrom(reader, scratch); err != nil {
		return scratch, err
	}

	if scratch, err = row.max_host.ReadFrom(reader, scratch); err != nil {
		return scratch, err
	}

	if scratch, err = row.max_count_host.ReadFrom(reader, scratch); err != nil {
		return scratch, err
	}
	if err := row.percentiles.ReadFrom(reader); err != nil {
		return scratch, err
	}
	if err := row.uniq_state.ReadFrom(reader); err != nil {
		return scratch, err
	}

	return scratch, nil
}

func ReadUInt8(r *bufio.Reader, buf []byte, dst *uint8) ([]byte, error) {
	buf = slices.Grow(buf, 1)[:1]
	if _, err := io.ReadFull(r, buf); err != nil {
		return buf, err
	}
	*dst = buf[0]
	return buf, nil
}

func ReadUInt32(r *bufio.Reader, buf []byte, dst *uint32) ([]byte, error) {
	buf = slices.Grow(buf, 4)[:4]
	if _, err := io.ReadFull(r, buf); err != nil {
		return buf, err
	}
	*dst = binary.LittleEndian.Uint32(buf)
	return buf, nil
}

func ReadInt32(r *bufio.Reader, buf []byte, dst *int32) ([]byte, error) {
	buf = slices.Grow(buf, 4)[:4]
	if _, err := io.ReadFull(r, buf); err != nil {
		return buf, err
	}
	*dst = int32(binary.LittleEndian.Uint32(buf))
	return buf, nil
}

func ReadFloat64(r *bufio.Reader, buf []byte, dst *float64) ([]byte, error) {
	buf = slices.Grow(buf, 8)[:8]
	if _, err := io.ReadFull(r, buf); err != nil {
		return buf, err
	}
	*dst = math.Float64frombits(binary.LittleEndian.Uint64(buf))
	return buf, nil
}

func readString(reader *bufio.Reader, buf []byte, dst *string) ([]byte, error) {
	n, err := binary.ReadUvarint(reader)
	if err != nil {
		return buf, err
	}
	buf = slices.Grow(buf, int(n))[:n]
	_, err = io.ReadFull(reader, buf)
	if err != nil {
		return buf, err
	}
	*dst = string(buf)
	return buf, nil
}

func (a *Aggregator) executeV3Query(query string, isSelect bool, body []byte, httpClient *http.Client) (io.ReadCloser, error) {
	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       a.config.KHAddr,
		User:       a.config.KHUser,
		Password:   a.config.KHPassword,
		Query:      query,
		Body:       body,
	}

	if isSelect {
		req.Format = a.migrationConfigV3.Format
	} else {
		req.UrlParams = map[string]string{"input_format_values_interpret_expressions": "0"}
	}
	log.Printf("[migration_v3] Executing query: %s", query)
	response, err := req.Execute(context.Background())
	if err != nil {
		log.Printf("[migration_v3] Error executing query: %v\nquery: %s", err, query)
		return nil, err
	}
	return response, nil
}

func (a *Aggregator) convertV3Response(v3Data io.Reader, output io.Writer, ts time.Time) (rowsProcessed int, tagsStringified int, err error) {
	reader := bufio.NewReaderSize(v3Data, 8192)
	rowData := make([]byte, 0, 4096)
	var v3row v3Row

	for {
		rowData = rowData[:0]
		var parseErr error
		if rowData, parseErr = parseV3RowOptimized(reader, rowData, &v3row); parseErr != nil {
			if errors.Is(parseErr, io.EOF) {
				// End of input, we're done
				log.Printf("[migration_v3] Reached EOF after processing %d rows", rowsProcessed)
				break
			}
			if errors.Is(parseErr, io.ErrUnexpectedEOF) {
				// Incomplete row, but we're using io.Reader so this shouldn't happen
				// unless the reader itself is incomplete
				log.Printf("[migration_v3] Unexpected EOF after processing %d rows: %v", rowsProcessed, parseErr)
				return rowsProcessed, tagsStringified, parseErr
			}
			log.Printf("[migration_v3] Parse error after processing %d rows: %v", rowsProcessed, parseErr)
			return rowsProcessed, tagsStringified, fmt.Errorf("failed to parse V2 row: %w", parseErr)
		}

		// quick verification that the read timestamp matches the target migration ts, to prevent writing wrong time ranges
		if ts.Unix() != int64(v3row.time) {
			return rowsProcessed, tagsStringified, fmt.Errorf("unexpected timestamp after processing %d rows: %d", rowsProcessed, v3row.time)
		}

		if !a.isRelevantMetric(v3row.metric) {
			continue
		}
		rawness, ok := a.migrationV3Data.isRawTagOfMetric[v3row.metric]
		if !ok {
			err := a.loadMetricTagRawness(v3row.metric)
			if err != nil {
				// shouldn't happen
				log.Printf("[migration_v3] Failed to load tag rawness for metric %d: %v", v3row.metric, err)
				return rowsProcessed, tagsStringified, fmt.Errorf("failed to load tag rawness for metric %d: %w", v3row.metric, err)
			}
			rawness = a.migrationV3Data.isRawTagOfMetric[v3row.metric]
		}

		rowData = rowData[:0]
		var stringified int
		rowData, stringified, err = a.encodeV3Row(rowData, &v3row, rawness)
		if err != nil {
			return rowsProcessed, tagsStringified, fmt.Errorf("failed to encode v3 row: %w", err)
		}

		if _, writeErr := output.Write(rowData); writeErr != nil {
			log.Printf("[migration_v3] Write error after processing %d rows: %v", rowsProcessed, writeErr)
			return rowsProcessed, tagsStringified, fmt.Errorf("failed to write converted row: %w", writeErr)
		}
		tagsStringified += stringified
		rowsProcessed++

	}
	return rowsProcessed, tagsStringified, nil
}

func (a *Aggregator) loadMetricTagRawness(metricId int32) error {
	var m *format.MetricMetaValue
	if metricId < 0 {
		m = format.BuiltinMetrics[metricId]
		if m == nil {
			return fmt.Errorf("unsupported negative metricId: %d (not present in builtin metrics list). shouldn't be migrated", metricId)
		}
	} else {
		m = a.metricStorage.GetMetaMetric(metricId)
		if m == nil {
			return fmt.Errorf("metric meta not found in storage: id=%d", metricId)
		}
	}

	if len(m.Tags) > 48 {
		return fmt.Errorf("[migration_v3] unexpected number of tags  %d", len(m.Tags))
	}

	rd := make([]bool, 48)
	for j, t := range m.Tags {
		if t.Raw() && j < len(rd) {
			rd[j] = true
		}
		if t.Raw64() && j+1 < len(rd) {
			rd[j+1] = true
		}
	}
	a.migrationV3Data.isRawTagOfMetric[metricId] = rd

	return nil
}

func (a *Aggregator) encodeV3Row(buf []byte, row *v3Row, isRawByTag []bool) (_ []byte, tagsStringified int, err error) {
	buf = rowbinary.AppendUint8(buf, row.index_type)
	buf = rowbinary.AppendInt32(buf, row.metric)
	buf = rowbinary.AppendUint32(buf, row.pre_tag)
	buf = rowbinary.AppendString(buf, row.pre_stag)
	buf = rowbinary.AppendDateTime(buf, time.Unix(int64(row.time), 0))

	for i := 0; i < 48; i++ {
		tag := row.tags[i]
		stag := row.stags[i]
		_, ok := a.migrationV3Data.replacementMappings[tag]
		if ok && isRawByTag != nil && !isRawByTag[i] {
			stag, ok = a.migrationV3Data.mappingsStorage.GetString(tag)
			if !ok {
				return nil, tagsStringified, fmt.Errorf("tag %d has to be replaced, but not found in mappings storage", tag)
			} else {
				tag = 0
				tagsStringified += 1
			}
		}
		buf = rowbinary.AppendInt32(buf, tag)
		buf = rowbinary.AppendString(buf, stag)
	}

	buf = rowbinary.AppendFloat64(buf, row.count)
	buf = rowbinary.AppendFloat64(buf, row.min)
	buf = rowbinary.AppendFloat64(buf, row.max)
	buf = rowbinary.AppendFloat64(buf, row.max_count)
	buf = rowbinary.AppendFloat64(buf, row.sum)
	buf = rowbinary.AppendFloat64(buf, row.sumsquare)

	//// host columns may contain underlying int values which should be interpreted as mappings
	stringifiedMinHost, err := a.replaceHostMappingIfNeeded(&row.min_host.ArgMinMaxStringFloat32)
	if err != nil {
		return nil, tagsStringified, fmt.Errorf("failed to replace min_host mapping: %w", err)
	}
	stringifiedMaxHost, err := a.replaceHostMappingIfNeeded(&row.max_host.ArgMinMaxStringFloat32)
	if err != nil {
		return nil, tagsStringified, fmt.Errorf("failed to replace max_host mapping: %w", err)
	}
	stringifiedMaxCountHost, err := a.replaceHostMappingIfNeeded(&row.max_count_host.ArgMinMaxStringFloat32)
	if err != nil {
		return nil, tagsStringified, fmt.Errorf("failed to replace max_count_host mapping: %w", err)
	}
	tagsStringified += stringifiedMinHost + stringifiedMaxHost + stringifiedMaxCountHost

	buf = row.min_host.MarshallAppend(buf)
	buf = row.max_host.MarshallAppend(buf)
	buf = row.max_count_host.MarshallAppend(buf)
	buf = row.percentiles.MarshallAppend(buf, 1)
	buf = row.uniq_state.MarshallAppend(buf)

	return buf, tagsStringified, nil
}

func (a *Aggregator) replaceHostMappingIfNeeded(hostArg *data_model.ArgMinMaxStringFloat32) (int, error) {
	// returns 1 if the mapping was decoded and replaced, else 0
	if hostArg.AsInt32 != 0 {
		_, presentInReplacementMappings := a.migrationV3Data.replacementMappings[hostArg.AsInt32]
		if !presentInReplacementMappings {
			return 0, nil
		}

		replacement, ok := a.migrationV3Data.mappingsStorage.GetString(hostArg.AsInt32)
		if ok {
			hostArg.AsString = replacement
			hostArg.AsInt32 = 0
			return 1, nil
		} else {
			return 0, fmt.Errorf("failed to map host value to string: int32 not found in mappings storage %d", hostArg.AsInt32)
		}
	}
	return 0, nil
}
