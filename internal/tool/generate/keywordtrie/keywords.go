package main

import (
	"github.com/tomarrell/lbadd/internal/parser/scanner/token"
)

var (
	keywordTokens = map[string]token.Type{
		"ABORT":             token.KeywordAbort,
		"ACTION":            token.KeywordAction,
		"ADD":               token.KeywordAdd,
		"AFTER":             token.KeywordAfter,
		"ALL":               token.KeywordAll,
		"ALTER":             token.KeywordAlter,
		"ALWAYS":            token.KeywordAlways,
		"ANALYZE":           token.KeywordAnalyze,
		"AND":               token.KeywordAnd,
		"AS":                token.KeywordAs,
		"ASC":               token.KeywordAsc,
		"ATTACH":            token.KeywordAttach,
		"AUTOINCREMENT":     token.KeywordAutoincrement,
		"BEFORE":            token.KeywordBefore,
		"BEGIN":             token.KeywordBegin,
		"BETWEEN":           token.KeywordBetween,
		"BY":                token.KeywordBy,
		"CASCADE":           token.KeywordCascade,
		"CASE":              token.KeywordCase,
		"CAST":              token.KeywordCast,
		"CHECK":             token.KeywordCheck,
		"COLLATE":           token.KeywordCollate,
		"COLUMN":            token.KeywordColumn,
		"COMMIT":            token.KeywordCommit,
		"CONFLICT":          token.KeywordConflict,
		"CONSTRAINT":        token.KeywordConstraint,
		"CREATE":            token.KeywordCreate,
		"CROSS":             token.KeywordCross,
		"CURRENT_DATE":      token.KeywordCurrentDate,
		"CURRENT_TIME":      token.KeywordCurrentTime,
		"CURRENT_TIMESTAMP": token.KeywordCurrentTimestamp,
		"CURRENT":           token.KeywordCurrent,
		"DATABASE":          token.KeywordDatabase,
		"DEFAULT":           token.KeywordDefault,
		"DEFERRABLE":        token.KeywordDeferrable,
		"DEFERRED":          token.KeywordDeferred,
		"DELETE":            token.KeywordDelete,
		"DESC":              token.KeywordDesc,
		"DETACH":            token.KeywordDetach,
		"DISTINCT":          token.KeywordDistinct,
		"DO":                token.KeywordDo,
		"DROP":              token.KeywordDrop,
		"EACH":              token.KeywordEach,
		"ELSE":              token.KeywordElse,
		"END":               token.KeywordEnd,
		"ESCAPE":            token.KeywordEscape,
		"EXCEPT":            token.KeywordExcept,
		"EXCLUDE":           token.KeywordExclude,
		"EXCLUSIVE":         token.KeywordExclusive,
		"EXISTS":            token.KeywordExists,
		"EXPLAIN":           token.KeywordExplain,
		"FAIL":              token.KeywordFail,
		"FILTER":            token.KeywordFilter,
		"FIRST":             token.KeywordFirst,
		"FOLLOWING":         token.KeywordFollowing,
		"FOR":               token.KeywordFor,
		"FOREIGN":           token.KeywordForeign,
		"FROM":              token.KeywordFrom,
		"FULL":              token.KeywordFull,
		"GLOB":              token.KeywordGlob,
		"GROUP":             token.KeywordGroup,
		"GROUPS":            token.KeywordGroups,
		"HAVING":            token.KeywordHaving,
		"IF":                token.KeywordIf,
		"IGNORE":            token.KeywordIgnore,
		"IMMEDIATE":         token.KeywordImmediate,
		"IN":                token.KeywordIn,
		"INDEX":             token.KeywordIndex,
		"INDEXED":           token.KeywordIndexed,
		"INITIALLY":         token.KeywordInitially,
		"INNER":             token.KeywordInner,
		"INSERT":            token.KeywordInsert,
		"INSTEAD":           token.KeywordInstead,
		"INTERSECT":         token.KeywordIntersect,
		"INTO":              token.KeywordInto,
		"IS":                token.KeywordIs,
		"ISNULL":            token.KeywordIsnull,
		"JOIN":              token.KeywordJoin,
		"KEY":               token.KeywordKey,
		"LAST":              token.KeywordLast,
		"LEFT":              token.KeywordLeft,
		"LIKE":              token.KeywordLike,
		"LIMIT":             token.KeywordLimit,
		"MATCH":             token.KeywordMatch,
		"NATURAL":           token.KeywordNatural,
		"NO":                token.KeywordNo,
		"NOT":               token.KeywordNot,
		"NOTHING":           token.KeywordNothing,
		"NOTNULL":           token.KeywordNotnull,
		"NULL":              token.KeywordNull,
		"NULLS":             token.KeywordNulls,
		"OF":                token.KeywordOf,
		"OFFSET":            token.KeywordOffset,
		"ON":                token.KeywordOn,
		"OR":                token.KeywordOr,
		"ORDER":             token.KeywordOrder,
		"OTHERS":            token.KeywordOthers,
		"OUTER":             token.KeywordOuter,
		"OVER":              token.KeywordOver,
		"PARTITION":         token.KeywordPartition,
		"PLAN":              token.KeywordPlan,
		"PRAGMA":            token.KeywordPragma,
		"PRECEDING":         token.KeywordPreceding,
		"PRIMARY":           token.KeywordPrimary,
		"QUERY":             token.KeywordQuery,
		"RAISE":             token.KeywordRaise,
		"RANGE":             token.KeywordRange,
		"RECURSIVE":         token.KeywordRecursive,
		"REFERENCES":        token.KeywordReferences,
		"REGEXP":            token.KeywordRegexp,
		"REINDEX":           token.KeywordReindex,
		"RELEASE":           token.KeywordRelease,
		"RENAME":            token.KeywordRename,
		"REPLACE":           token.KeywordReplace,
		"RESTRICT":          token.KeywordRestrict,
		"RIGHT":             token.KeywordRight,
		"ROLLBACK":          token.KeywordRollback,
		"ROW":               token.KeywordRow,
		"ROWS":              token.KeywordRows,
		"SAVEPOINT":         token.KeywordSavepoint,
		"SELECT":            token.KeywordSelect,
		"SET":               token.KeywordSet,
		"STORED":            token.KeywordStored,
		"TABLE":             token.KeywordTable,
		"TEMP":              token.KeywordTemp,
		"TEMPORARY":         token.KeywordTemporary,
		"THEN":              token.KeywordThen,
		"TIES":              token.KeywordTies,
		"TO":                token.KeywordTo,
		"TRANSACTION":       token.KeywordTransaction,
		"TRIGGER":           token.KeywordTrigger,
		"UNBOUNDED":         token.KeywordUnbounded,
		"UNION":             token.KeywordUnion,
		"UNIQUE":            token.KeywordUnique,
		"UPDATE":            token.KeywordUpdate,
		"USING":             token.KeywordUsing,
		"VACUUM":            token.KeywordVacuum,
		"VALUES":            token.KeywordValues,
		"VIEW":              token.KeywordView,
		"VIRTUAL":           token.KeywordVirtual,
		"WHEN":              token.KeywordWhen,
		"WHERE":             token.KeywordWhere,
		"WINDOW":            token.KeywordWindow,
		"WITH":              token.KeywordWith,
		"WITHOUT":           token.KeywordWithout,
	}
)
