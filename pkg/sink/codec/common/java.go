// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.orglicensesLICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import "github.com/pingcap/tidb/pkg/parser/mysql"

// JavaSQLType is the sql type defined in class java.sql.Types in java sdk
type JavaSQLType int32

// jdk 1.8
const (
	JavaSQLTypeBIT                     JavaSQLType = -7
	JavaSQLTypeTINYINT                 JavaSQLType = -6
	JavaSQLTypeSMALLINT                JavaSQLType = 5
	JavaSQLTypeINTEGER                 JavaSQLType = 4
	JavaSQLTypeBIGINT                  JavaSQLType = -5
	JavaSQLTypeREAL                    JavaSQLType = 7
	JavaSQLTypeDOUBLE                  JavaSQLType = 8
	JavaSQLTypeDECIMAL                 JavaSQLType = 3
	JavaSQLTypeCHAR                    JavaSQLType = 1
	JavaSQLTypeVARCHAR                 JavaSQLType = 12
	JavaSQLTypeDATE                    JavaSQLType = 91
	JavaSQLTypeTIME                    JavaSQLType = 92
	JavaSQLTypeTIMESTAMP               JavaSQLType = 93
	JavaSQLTypeBINARY                  JavaSQLType = -2
	JavaSQLTypeVARBINARY               JavaSQLType = -3
	JavaSQLTypeLONGVARBINARY           JavaSQLType = -4
	JavaSQLTypeNULL                    JavaSQLType = 0
	JavaSQLTypeBLOB                    JavaSQLType = 2004
	JavaSQLTypeCLOB                    JavaSQLType = 2005
	JavaSQLTypeFLOAT                   JavaSQLType = 6
	JavaSQLTypeNUMERIC                 JavaSQLType = 2
	JavaSQLTypeOTHER                   JavaSQLType = 1111
	JavaSQLTypeNCHAR                   JavaSQLType = -15
	JavaSQLTypeTIMESTAMP_WITH_TIMEZONE JavaSQLType = 2014 //nolint

	// unused
	// JavaSQLTypeLONGVARCHAR             JavaSQLType = -1
	// JavaSQLTypeJAVA_OBJECT             JavaSQLType = 2000
	// JavaSQLTypeDISTINCT                JavaSQLType = 2001
	// JavaSQLTypeSTRUCT                  JavaSQLType = 2002
	// JavaSQLTypeARRAY                   JavaSQLType = 2003
	// JavaSQLTypeREF                     JavaSQLType = 2006
	// JavaSQLTypeDATALINK                JavaSQLType = 70
	// JavaSQLTypeBOOLEAN                 JavaSQLType = 16
	// JavaSQLTypeROWID                   JavaSQLType = -8
	// JavaSQLTypeNVARCHAR                JavaSQLType = -9
	// JavaSQLTypeLONGNVARCHAR            JavaSQLType = -16
	// JavaSQLTypeNCLOB                   JavaSQLType = 2011
	// JavaSQLTypeSQLXML                  JavaSQLType = 2009
	// JavaSQLTypeREF_CURSOR              JavaSQLType = 2012
	// JavaSQLTypeTIME_WITH_TIMEZONE      JavaSQLType = 2013
)

// MySQLType2JdbcType converts the mysql protocol types to jdbc type
func MySQLType2JdbcType(mysqlType byte, isBinary bool) JavaSQLType {
	switch mysqlType {
	case mysql.TypeTiny, mysql.TypeShort:
		return JavaSQLTypeSMALLINT

	case mysql.TypeLong, mysql.TypeInt24, mysql.TypeYear:
		return JavaSQLTypeINTEGER

	case mysql.TypeFloat:
		return JavaSQLTypeFLOAT

	case mysql.TypeDouble:
		return JavaSQLTypeDOUBLE

	case mysql.TypeNull:
		return JavaSQLTypeNULL

	case mysql.TypeNewDecimal:
		return JavaSQLTypeDECIMAL // equal to JavaSQLTypeNUMERIC

	case mysql.TypeTimestamp:
		return JavaSQLTypeTIMESTAMP_WITH_TIMEZONE //nolint

	case mysql.TypeDatetime:
		return JavaSQLTypeTIMESTAMP

	case mysql.TypeLonglong:
		return JavaSQLTypeBIGINT

	case mysql.TypeDate, mysql.TypeNewDate:
		return JavaSQLTypeDATE

	case mysql.TypeDuration:
		return JavaSQLTypeTIME

	case mysql.TypeEnum, mysql.TypeSet:
		return JavaSQLTypeCHAR

	case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		if isBinary {
			return JavaSQLTypeBLOB
		}
		return JavaSQLTypeVARCHAR

	case mysql.TypeVarString, mysql.TypeVarchar:
		if isBinary {
			return JavaSQLTypeVARBINARY
		}
		return JavaSQLTypeVARCHAR

	case mysql.TypeString:
		if isBinary {
			return JavaSQLTypeBINARY
		}
		return JavaSQLTypeCHAR
		// return JavaSQLTypeNCHAR

	// Geometry is not supported, this should not hit.
	case mysql.TypeGeometry:
		return JavaSQLTypeOTHER

	case mysql.TypeBit:
		return JavaSQLTypeBIT

	case mysql.TypeJSON:
		return JavaSQLTypeOTHER

	default:
		return JavaSQLTypeVARCHAR
	}
}
