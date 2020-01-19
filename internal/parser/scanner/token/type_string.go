// Code generated by "stringer -type=Type"; DO NOT EDIT.

package token

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[Unknown-0]
	_ = x[Error-1]
	_ = x[EOF-2]
	_ = x[ABORT-3]
	_ = x[ACTION-4]
	_ = x[ADD-5]
	_ = x[AFTER-6]
	_ = x[ALL-7]
	_ = x[ALTER-8]
	_ = x[ANALYZE-9]
	_ = x[AND-10]
	_ = x[AS-11]
	_ = x[ASC-12]
	_ = x[ATTACH-13]
	_ = x[AUTOINCREMENT-14]
	_ = x[BEFORE-15]
	_ = x[BEGIN-16]
	_ = x[BETWEEN-17]
	_ = x[BY-18]
	_ = x[CASCADE-19]
	_ = x[CASE-20]
	_ = x[CAST-21]
	_ = x[CHECK-22]
	_ = x[COLLATE-23]
	_ = x[COLUMN-24]
	_ = x[COMMIT-25]
	_ = x[CONFLICT-26]
	_ = x[CONSTRAINT-27]
	_ = x[CREATE-28]
	_ = x[CROSS-29]
	_ = x[CURRENT-30]
	_ = x[CURRENT_DATE-31]
	_ = x[CURRENT_TIME-32]
	_ = x[CURRENT_TIMESTAMP-33]
	_ = x[DATABASE-34]
	_ = x[DEFAULT-35]
	_ = x[DEFERRABLE-36]
	_ = x[DEFERRED-37]
	_ = x[DELETE-38]
	_ = x[DESC-39]
	_ = x[DETACH-40]
	_ = x[DISTINCT-41]
	_ = x[DO-42]
	_ = x[DROP-43]
	_ = x[EACH-44]
	_ = x[ELSE-45]
	_ = x[END-46]
	_ = x[ESCAPE-47]
	_ = x[EXCEPT-48]
	_ = x[EXCLUDE-49]
	_ = x[EXCLUSIVE-50]
	_ = x[EXISTS-51]
	_ = x[EXPLAIN-52]
	_ = x[FAIL-53]
	_ = x[FILTER-54]
	_ = x[FIRST-55]
	_ = x[FOLLOWING-56]
	_ = x[FOR-57]
	_ = x[FOREIGN-58]
	_ = x[FROM-59]
	_ = x[FULL-60]
	_ = x[GLOB-61]
	_ = x[GROUP-62]
	_ = x[GROUPS-63]
	_ = x[HAVING-64]
	_ = x[IF-65]
	_ = x[IGNORE-66]
	_ = x[IMMEDIATE-67]
	_ = x[IN-68]
	_ = x[INDEX-69]
	_ = x[INDEXED-70]
	_ = x[INITIALLY-71]
	_ = x[INNER-72]
	_ = x[INSERT-73]
	_ = x[INSTEAD-74]
	_ = x[INTERSECT-75]
	_ = x[INTO-76]
	_ = x[IS-77]
	_ = x[ISNULL-78]
	_ = x[JOIN-79]
	_ = x[KEY-80]
	_ = x[LAST-81]
	_ = x[LEFT-82]
	_ = x[LIKE-83]
	_ = x[LIMIT-84]
	_ = x[MATCH-85]
	_ = x[NATURAL-86]
	_ = x[NO-87]
	_ = x[NOT-88]
	_ = x[NOTHING-89]
	_ = x[NOTNULL-90]
	_ = x[NULL-91]
	_ = x[NULLS-92]
	_ = x[OF-93]
	_ = x[OFFSET-94]
	_ = x[ON-95]
	_ = x[OR-96]
	_ = x[ORDER-97]
	_ = x[OTHERS-98]
	_ = x[OUTER-99]
	_ = x[OVER-100]
	_ = x[PARTITION-101]
	_ = x[PLAN-102]
	_ = x[PRAGMA-103]
	_ = x[PRECEDING-104]
	_ = x[PRIMARY-105]
	_ = x[QUERY-106]
	_ = x[RAISE-107]
	_ = x[RANGE-108]
	_ = x[RECURSIVE-109]
	_ = x[REFERENCES-110]
	_ = x[REGEXP-111]
	_ = x[REINDEX-112]
	_ = x[RELEASE-113]
	_ = x[RENAME-114]
	_ = x[REPLACE-115]
	_ = x[RESTRICT-116]
	_ = x[RIGHT-117]
	_ = x[ROLLBACK-118]
	_ = x[ROW-119]
	_ = x[ROWS-120]
	_ = x[SAVEPOINT-121]
	_ = x[SELECT-122]
	_ = x[SET-123]
	_ = x[TABLE-124]
	_ = x[TEMP-125]
	_ = x[TEMPORARY-126]
	_ = x[THEN-127]
	_ = x[TIES-128]
	_ = x[TO-129]
	_ = x[TRANSACTION-130]
	_ = x[TRIGGER-131]
	_ = x[UNBOUNDED-132]
	_ = x[UNION-133]
	_ = x[UNIQUE-134]
	_ = x[UPDATE-135]
	_ = x[USING-136]
	_ = x[VACUUM-137]
	_ = x[VALUES-138]
	_ = x[VIEW-139]
	_ = x[VIRTUAL-140]
	_ = x[WHEN-141]
	_ = x[WHERE-142]
	_ = x[WINDOW-143]
	_ = x[WITH-144]
	_ = x[WITHOUT-145]
}

const _Type_name = "UnknownErrorEOFABORTACTIONADDAFTERALLALTERANALYZEANDASASCATTACHAUTOINCREMENTBEFOREBEGINBETWEENBYCASCADECASECASTCHECKCOLLATECOLUMNCOMMITCONFLICTCONSTRAINTCREATECROSSCURRENTCURRENT_DATECURRENT_TIMECURRENT_TIMESTAMPDATABASEDEFAULTDEFERRABLEDEFERREDDELETEDESCDETACHDISTINCTDODROPEACHELSEENDESCAPEEXCEPTEXCLUDEEXCLUSIVEEXISTSEXPLAINFAILFILTERFIRSTFOLLOWINGFORFOREIGNFROMFULLGLOBGROUPGROUPSHAVINGIFIGNOREIMMEDIATEININDEXINDEXEDINITIALLYINNERINSERTINSTEADINTERSECTINTOISISNULLJOINKEYLASTLEFTLIKELIMITMATCHNATURALNONOTNOTHINGNOTNULLNULLNULLSOFOFFSETONORORDEROTHERSOUTEROVERPARTITIONPLANPRAGMAPRECEDINGPRIMARYQUERYRAISERANGERECURSIVEREFERENCESREGEXPREINDEXRELEASERENAMEREPLACERESTRICTRIGHTROLLBACKROWROWSSAVEPOINTSELECTSETTABLETEMPTEMPORARYTHENTIESTOTRANSACTIONTRIGGERUNBOUNDEDUNIONUNIQUEUPDATEUSINGVACUUMVALUESVIEWVIRTUALWHENWHEREWINDOWWITHWITHOUT"

var _Type_index = [...]uint16{0, 7, 12, 15, 20, 26, 29, 34, 37, 42, 49, 52, 54, 57, 63, 76, 82, 87, 94, 96, 103, 107, 111, 116, 123, 129, 135, 143, 153, 159, 164, 171, 183, 195, 212, 220, 227, 237, 245, 251, 255, 261, 269, 271, 275, 279, 283, 286, 292, 298, 305, 314, 320, 327, 331, 337, 342, 351, 354, 361, 365, 369, 373, 378, 384, 390, 392, 398, 407, 409, 414, 421, 430, 435, 441, 448, 457, 461, 463, 469, 473, 476, 480, 484, 488, 493, 498, 505, 507, 510, 517, 524, 528, 533, 535, 541, 543, 545, 550, 556, 561, 565, 574, 578, 584, 593, 600, 605, 610, 615, 624, 634, 640, 647, 654, 660, 667, 675, 680, 688, 691, 695, 704, 710, 713, 718, 722, 731, 735, 739, 741, 752, 759, 768, 773, 779, 785, 790, 796, 802, 806, 813, 817, 822, 828, 832, 839}

func (i Type) String() string {
	if i >= Type(len(_Type_index)-1) {
		return "Type(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _Type_name[_Type_index[i]:_Type_index[i+1]]
}
