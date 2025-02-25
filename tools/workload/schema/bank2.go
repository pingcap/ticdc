// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schema

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"
)

const createTableSQL = `CREATE TABLE acct_statement_head_info (
	local_id bigint(20) NOT NULL,
	stmt_head_id bigint(20) NOT NULL,
	stmt_date date NOT NULL,
	part_no int(11) NOT NULL DEFAULT '0',
	crcd_acct_no varchar(16) NOT NULL DEFAULT '',
	acct_seq_no varchar(3) NOT NULL DEFAULT '',
	acct_ccy varchar(3) NOT NULL DEFAULT '',
	crcd_org_no varchar(3) NOT NULL DEFAULT '',
	crcd_cardholder_no varchar(16) NOT NULL DEFAULT '',
	prev_stmt_head_id bigint(20) NOT NULL DEFAULT '0',
	repay_expire_date date DEFAULT NULL,
	late_chg_coll_date date DEFAULT NULL,
	grace_date date DEFAULT NULL,
	total_amt_due decimal(13,2) NOT NULL DEFAULT '0.00',
	monthly_pymt decimal(13,2) NOT NULL DEFAULT '0.00',
	delq_total_amt decimal(13,2) NOT NULL DEFAULT '0.00',
	overlimit_acctt_amt decimal(13,2) NOT NULL DEFAULT '0.00',
	cur_pymt_amt decimal(13,2) NOT NULL DEFAULT '0.00',
	begin_bal decimal(13,2) NOT NULL DEFAULT '0.00',
	debit_trx_amt decimal(13,2) NOT NULL DEFAULT '0.00',
	cr_trx_amt decimal(13,2) NOT NULL DEFAULT '0.00',
	dispute_bal decimal(13,2) NOT NULL DEFAULT '0.00',
	dispute_trx_count int(11) NOT NULL DEFAULT '0',
	crcd_stmt_no smallint(6) NOT NULL,
	acct_rtl_limit decimal(13,0) NOT NULL DEFAULT '0',
	acct_avail_rtl_limit decimal(13,2) NOT NULL DEFAULT '0.00',
	stmt_cycle_days int(11) NOT NULL DEFAULT '0',
	cash_adv_amt decimal(13,2) NOT NULL DEFAULT '0.00',
	intr_amt decimal(13,2) NOT NULL DEFAULT '0.00',
	stmt_type varchar(1) NOT NULL DEFAULT '',
	debit_cr_out_of_bal_flag varchar(1) NOT NULL DEFAULT '',
	acct_prev_block_code varchar(3) NOT NULL DEFAULT '',
	stmt_mail_flag varchar(1) NOT NULL DEFAULT '',
	only_stmt_head_record_flag varchar(1) NOT NULL DEFAULT '',
	bp_rtl_intr_amt decimal(13,2) NOT NULL DEFAULT '0.00',
	bp_cash_adv_intr_amt decimal(13,2) NOT NULL DEFAULT '0.00',
	acct_sts varchar(1) NOT NULL DEFAULT '',
	acct_cur_block_code varchar(3) NOT NULL DEFAULT '',
	cycle_due int(11) NOT NULL DEFAULT '0',
	trx_count int(11) NOT NULL DEFAULT '0',
	begin_rtl_bal decimal(13,2) NOT NULL DEFAULT '0.00',
	acc_rtl_bal decimal(13,2) NOT NULL DEFAULT '0.00',
	rtl_days_with_bal smallint(6) NOT NULL DEFAULT '0',
	rtl_pymt_amt decimal(13,2) NOT NULL DEFAULT '0.00',
	rtl_pymt_rvsl_amt decimal(13,2) NOT NULL DEFAULT '0.00',
	rtl_debit_trx_amt decimal(13,2) NOT NULL DEFAULT '0.00',
	rtl_debit_trx_count int(11) NOT NULL DEFAULT '0',
	rtl_cr_trx_amt decimal(13,2) NOT NULL DEFAULT '0.00',
	rtl_cr_trx_count int(11) NOT NULL DEFAULT '0',
	rtl_serv_chg decimal(13,2) NOT NULL DEFAULT '0.00',
	rtl_trx_fee_amt decimal(13,2) NOT NULL DEFAULT '0.00',
	rtl_intr_amt decimal(13,2) NOT NULL DEFAULT '0.00',
	rtl_mis_fee decimal(13,2) NOT NULL DEFAULT '0.00',
	rtl_annual_fee decimal(13,2) NOT NULL DEFAULT '0.00',
	stmt_rtl_bal decimal(13,2) NOT NULL DEFAULT '0.00',
	rtl_dispute_amt decimal(13,2) NOT NULL DEFAULT '0.00',
	cash_adv_begin_bal decimal(13,2) NOT NULL DEFAULT '0.00',
	acc_cash_bal decimal(13,2) NOT NULL DEFAULT '0.00',
	cash_adv_days_with_bal int(11) NOT NULL DEFAULT '0',
	cash_adv_pymt_amt decimal(13,2) NOT NULL DEFAULT '0.00',
	ctd_cash_adv_pymt_rvsl decimal(13,2) NOT NULL DEFAULT '0.00',
	cash_adv_debit_trx_amt decimal(13,2) NOT NULL DEFAULT '0.00',
	cash_adv_debit_trx_count int(11) NOT NULL DEFAULT '0',
	cash_adv_cr_trx_amt decimal(13,2) NOT NULL DEFAULT '0.00',
	cash_adv_cr_trx_count int(11) NOT NULL DEFAULT '0',
	cash_adv_serv_chg decimal(13,2) NOT NULL DEFAULT '0.00',
	cash_adv_trx_fee_amt decimal(13,2) NOT NULL DEFAULT '0.00',
	cash_adv_intr_amt decimal(13,2) NOT NULL DEFAULT '0.00',
	stmt_cash_bal decimal(13,2) NOT NULL DEFAULT '0.00',
	cash_adv_dispute_amt decimal(13,2) NOT NULL DEFAULT '0.00',
	cash_adv_limit decimal(13,2) NOT NULL DEFAULT '0.00',
	avail_cash_adv_limit decimal(13,2) NOT NULL DEFAULT '0.00',
	agent_bank_no int(11) NOT NULL DEFAULT '0',
	dd_flag varchar(1) NOT NULL DEFAULT '',
	prev_stmt_rmn_intr decimal(13,2) NOT NULL DEFAULT '0.00',
	write_off_sts_flag varchar(1) NOT NULL DEFAULT '',
	prev_rtl_prin decimal(13,2) NOT NULL DEFAULT '0.00',
	cur_stmt_bal decimal(13,2) NOT NULL DEFAULT '0.00',
	ctd_stmt_stat_cycle int(11) NOT NULL DEFAULT '0',
	prev_stmt_date date DEFAULT NULL,
	rtl_intr_rate decimal(8,7) NOT NULL DEFAULT '0.0000000',
	late_chg decimal(13,2) NOT NULL DEFAULT '0.00',
	cash_bal_1_cur_intr_rate decimal(8,7) NOT NULL DEFAULT '0.0000000',
	global_trx_jrn_no varchar(37) DEFAULT NULL,
	src_cnsmr_sys_id varchar(4) DEFAULT NULL,
	local_sys_jrn_no varchar(32) DEFAULT NULL,
	biz_date date DEFAULT NULL,
	trx_date date DEFAULT NULL,
	trx_time time DEFAULT NULL,
	biz_scene_encod varchar(10) DEFAULT NULL,
	data_pos varchar(10) NOT NULL DEFAULT '',
	create_tlr_no varchar(32) NOT NULL DEFAULT '',
	upd_tlr_no varchar(32) NOT NULL DEFAULT '',
	create_tlr_org_no varchar(10) NOT NULL DEFAULT '',
	upd_tlr_org_no varchar(10) NOT NULL DEFAULT '',
	biz_upd_ts datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
	ver_no int(11) NOT NULL DEFAULT '0',
	create_ts datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
	upd_ts datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
	acct_bank_no varchar(4) NOT NULL DEFAULT '',
	acct_branch_no varchar(5) NOT NULL DEFAULT '',
	migr_flag varchar(1) NOT NULL DEFAULT '0',
	acct_chg_off_sts varchar(1) NOT NULL DEFAULT '',
	binlog_insert_ts datetime(6) NOT NULL DEFAULT '1900-01-01 00:00:00.000000',
	del_flag varchar(1) NOT NULL DEFAULT 'N',
	data_version varchar(256) DEFAULT NULL,
	prev_dir_to_cust_rtl_prin decimal(13,2) DEFAULT NULL,
	last_term_spec_whls_csm_prin decimal(13,2) DEFAULT NULL,
	last_term_stmt_forbid_instalt_prin_amt decimal(13,2) DEFAULT NULL,
	PRIMARY KEY (crcd_acct_no,stmt_date,acct_seq_no,stmt_head_id) /*T![clustered_index] CLUSTERED */,
	KEY upd_ts_del_flag_idx (upd_ts,data_pos,del_flag),
	KEY acct_statement_head_info_idx_1 (crcd_cardholder_no,stmt_date),
	KEY acct_statement_head_info_idx_2 (stmt_head_id,stmt_date)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`

const createLogTableSQL = `CREATE TABLE acct_interest_log_hist (
  id bigint(20) NOT NULL,
  local_id bigint(20) NOT NULL,
  create_date date NOT NULL,
  part_no int(11) NOT NULL DEFAULT '0',
  crcd_acct_no varchar(16) NOT NULL DEFAULT '',
  acct_seq_no varchar(3) NOT NULL DEFAULT '',
  acct_ccy varchar(3) NOT NULL DEFAULT '',
  crcd_org_no varchar(3) NOT NULL DEFAULT '',
  crcd_cardholder_no varchar(16) NOT NULL DEFAULT '',
  trx_sub_acct_no bigint(20) NOT NULL DEFAULT '0',
  trx_type varchar(4) NOT NULL DEFAULT '',
  intr_acc_cut_off_date date DEFAULT NULL,
  this_times_acc_bal decimal(13,2) NOT NULL DEFAULT '0.00',
  acc_intr_acr_days int(11) NOT NULL DEFAULT '0',
  cur_intr_rate decimal(8,7) NOT NULL DEFAULT '0.0000000',
  pt_id varchar(6) NOT NULL DEFAULT '',
  upd_prev_acc_fin_chg decimal(13,6) NOT NULL DEFAULT '0.000000',
  upd_after_acc_fin_chg decimal(13,6) NOT NULL DEFAULT '0.000000',
  carry_intr_day date DEFAULT NULL,
  perdiem decimal(13,6) NOT NULL DEFAULT '0.000000',
  trx_sub_acct_type varchar(6) NOT NULL DEFAULT '',
  stmt_date date DEFAULT NULL,
  global_trx_jrn_no varchar(37) DEFAULT NULL,
  src_cnsmr_sys_id varchar(4) DEFAULT NULL,
  local_sys_jrn_no varchar(32) DEFAULT NULL,
  biz_date date NOT NULL,
  trx_date date DEFAULT NULL,
  trx_time time DEFAULT NULL,
  biz_scene_encod varchar(10) NOT NULL DEFAULT '',
  data_pos varchar(10) NOT NULL DEFAULT '',
  create_tlr_no varchar(32) NOT NULL DEFAULT '',
  upd_tlr_no varchar(32) NOT NULL DEFAULT '',
  create_tlr_org_no varchar(10) NOT NULL DEFAULT '',
  upd_tlr_org_no varchar(10) NOT NULL DEFAULT '',
  biz_upd_ts datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  ver_no int(11) NOT NULL DEFAULT '0',
  create_ts datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  upd_ts datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  fund_type varchar(4) NOT NULL DEFAULT '',
  migr_flag varchar(1) NOT NULL DEFAULT '0',
  bal_sts varchar(1) NOT NULL DEFAULT '',
  once_waive_intr_flag varchar(1) NOT NULL DEFAULT '',
  binlog_insert_ts datetime(6) NOT NULL DEFAULT '1900-01-01 00:00:00.000000',
  del_flag varchar(1) NOT NULL DEFAULT 'N',
  data_version varchar(256) DEFAULT NULL,
  PRIMARY KEY (crcd_acct_no,create_date,id) /*T![clustered_index] NONCLUSTERED */,
  KEY acct_interest_log_hist_idx_1 (crcd_acct_no,stmt_date,id),
  KEY upd_ts_del_flag_idx (upd_ts,data_pos,del_flag)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T! SHARD_ROW_ID_BITS=9 PRE_SPLIT_REGIONS=9 */`

type Bank2Workload struct {
	infoTableInsertSQL string
	logTableInsertSQL  string
}

func NewBank2Workload() Workload {
	var builder strings.Builder
	builder.WriteString("insert into acct_statement_head_info (crcd_acct_no , stmt_date , acct_seq_no , stmt_head_id ,bp_rtl_intr_amt , late_chg_coll_date , delq_total_amt , prev_dir_to_cust_rtl_prin , acct_branch_no , create_ts , agent_bank_no , stmt_cycle_days , cash_adv_days_with_bal , acct_avail_rtl_limit , avail_cash_adv_limit , dispute_trx_count , rtl_intr_rate , only_stmt_head_record_flag , total_amt_due , upd_tlr_org_no , cash_adv_pymt_amt , acct_cur_block_code , stmt_mail_flag , local_id , debit_cr_out_of_bal_flag , binlog_insert_ts , acct_prev_block_code , src_cnsmr_sys_id , part_no , dd_flag , migr_flag , acct_ccy , rtl_debit_trx_count , intr_amt , stmt_cash_bal , write_off_sts_flag , prev_stmt_rmn_intr , create_tlr_org_no , upd_ts , debit_trx_amt , cash_adv_intr_amt , acc_rtl_bal , acct_rtl_limit , cash_adv_begin_bal , biz_date , late_chg , rtl_serv_chg , stmt_type , cash_bal_1_cur_intr_rate , begin_bal , cur_stmt_bal , rtl_days_with_bal , cash_adv_debit_trx_amt , cash_adv_amt , repay_expire_date , rtl_cr_trx_count , biz_scene_encod , rtl_dispute_amt , last_term_stmt_forbid_instalt_prin_amt , cash_adv_limit , monthly_pymt , rtl_trx_fee_amt , data_version , rtl_mis_fee , dispute_bal , ctd_cash_adv_pymt_rvsl , data_pos , rtl_debit_trx_amt , stmt_rtl_bal , last_term_spec_whls_csm_prin , del_flag , acct_chg_off_sts , cash_adv_debit_trx_count , bp_cash_adv_intr_amt , rtl_pymt_amt , trx_date , cur_pymt_amt , local_sys_jrn_no , prev_stmt_head_id , cash_adv_dispute_amt , rtl_pymt_rvsl_amt , cash_adv_cr_trx_amt , crcd_stmt_no , trx_count , global_trx_jrn_no , cash_adv_trx_fee_amt , cash_adv_serv_chg , overlimit_acctt_amt , crcd_cardholder_no ,  begin_rtl_bal , prev_rtl_prin , rtl_annual_fee , acct_bank_no , acct_sts , cr_trx_amt , crcd_org_no , rtl_intr_amt , rtl_cr_trx_amt , cycle_due , biz_upd_ts , prev_stmt_date , grace_date , trx_time , ctd_stmt_stat_cycle , cash_adv_cr_trx_count , upd_tlr_no , ver_no , create_tlr_no , acc_cash_bal ) values ")
	for r := 0; r < 200; r++ {
		if r != 0 {
			builder.WriteString(",")
		}
		builder.WriteString("(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
	}
	infoTableInsertSQL := builder.String()

	builder.Reset()

	builder.WriteString("insert into acct_interest_log_hist (crcd_acct_no , create_date , id , global_trx_jrn_no , perdiem , trx_sub_acct_no , carry_intr_day , fund_type , data_version , bal_sts , create_ts , this_times_acc_bal , acc_intr_acr_days , intr_acc_cut_off_date , crcd_cardholder_no , create_tlr_org_no , upd_ts , pt_id , upd_tlr_org_no , data_pos , trx_type , biz_date , del_flag , upd_prev_acc_fin_chg , local_id , binlog_insert_ts , crcd_org_no , stmt_date , trx_date , biz_upd_ts , cur_intr_rate ,  local_sys_jrn_no , src_cnsmr_sys_id , part_no , acct_seq_no , once_waive_intr_flag , migr_flag , trx_time , acct_ccy , upd_after_acc_fin_chg , upd_tlr_no , biz_scene_encod , ver_no , create_tlr_no , trx_sub_acct_type ) values ")
	for r := 0; r < 200; r++ {
		if r != 0 {
			builder.WriteString(",")
		}
		builder.WriteString("(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
	}
	logTableInsertSQL := builder.String()

	workload := &Bank2Workload{infoTableInsertSQL: infoTableInsertSQL, logTableInsertSQL: logTableInsertSQL}
	return workload
}
func (c *Bank2Workload) BuildCreateTableStatement(n int) string {
	switch n {
	case 0: // acct_statement_head_info
		return createTableSQL
	case 1: // acct_interest_log_hist
		return createLogTableSQL
	default:
		panic("unknown table")
	}
}

func (c *Bank2Workload) BuildInsertSql(tableN int, batchSize int) string {
	panic("unimplemented")
}

func (c *Bank2Workload) BuildUpdateSql(opts UpdateOption) string {
	panic("unimplemented")
}

var valuesPool = sync.Pool{
	New: func() interface{} {
		return make([]interface{}, 0, 4)
	},
}

type SqlValue struct {
	Sql    string
	Values []interface{}
}

var largeValuesPool = sync.Pool{
	New: func() interface{} {
		return make([]interface{}, 0, 120*200)
	},
}

func (c *Bank2Workload) BuildInsertSqlWithValues(tableN int, batchSize int) (string, []interface{}) {
	switch tableN {
	case 0: // acct_statement_head_info
		nonPrimaryKeyValues := generateNonPrimaryValuesForTable() // to reduce time, these field we keep same for
		sql := c.infoTableInsertSQL
		rand.Seed(time.Now().UnixNano())

		values := valuesPool.Get().([]interface{})
		defer valuesPool.Put(values[:0])
		// 200 rows one txn
		for r := 0; r < 200; r++ {
			values = append(values, generatePrimaryValuesForTable()...)
			values = append(values, nonPrimaryKeyValues...)
		}

		return sql, values
	case 1: // acct_interest_log_hist
		sql := c.logTableInsertSQL
		nonPrimaryKeyValues := generateNonPrimaryValuesForLogTable()
		rand.Seed(time.Now().UnixNano())

		values := valuesPool.Get().([]interface{})
		defer valuesPool.Put(values[:0])
		// 200 rows one txn
		for r := 0; r < 200; r++ {
			values = append(values, generatePrimaryValuesForLogTable()...)
			values = append(values, nonPrimaryKeyValues...)
		}

		return sql, values
	default:
		panic("unknown table")
	}
	//return sql, values
}

func (c *Bank2Workload) BuildUpdateSqlWithValues(opts UpdateOption) (string, []interface{}) {
	rand.Seed(time.Now().UnixNano())
	var sql string
	values := make([]interface{}, 0, 120)
	switch opts.Table {
	case 0: // acct_statement_head_info
		sql = "insert into acct_statement_head_info (crcd_acct_no , stmt_date , acct_seq_no , stmt_head_id ,bp_rtl_intr_amt , late_chg_coll_date , delq_total_amt , prev_dir_to_cust_rtl_prin , acct_branch_no , create_ts , agent_bank_no , stmt_cycle_days , cash_adv_days_with_bal , acct_avail_rtl_limit , avail_cash_adv_limit , dispute_trx_count , rtl_intr_rate , only_stmt_head_record_flag , total_amt_due , upd_tlr_org_no , cash_adv_pymt_amt , acct_cur_block_code , stmt_mail_flag , local_id , debit_cr_out_of_bal_flag , binlog_insert_ts , acct_prev_block_code , src_cnsmr_sys_id , part_no , dd_flag , migr_flag , acct_ccy , rtl_debit_trx_count , intr_amt , stmt_cash_bal , write_off_sts_flag , prev_stmt_rmn_intr , create_tlr_org_no , upd_ts , debit_trx_amt , cash_adv_intr_amt , acc_rtl_bal , acct_rtl_limit , cash_adv_begin_bal , biz_date , late_chg , rtl_serv_chg , stmt_type , cash_bal_1_cur_intr_rate , begin_bal , cur_stmt_bal , rtl_days_with_bal , cash_adv_debit_trx_amt , cash_adv_amt , repay_expire_date , rtl_cr_trx_count , biz_scene_encod , rtl_dispute_amt , last_term_stmt_forbid_instalt_prin_amt , cash_adv_limit , monthly_pymt , rtl_trx_fee_amt , data_version , rtl_mis_fee , dispute_bal , ctd_cash_adv_pymt_rvsl , data_pos , rtl_debit_trx_amt , stmt_rtl_bal , last_term_spec_whls_csm_prin , del_flag , acct_chg_off_sts , cash_adv_debit_trx_count , bp_cash_adv_intr_amt , rtl_pymt_amt , trx_date , cur_pymt_amt , local_sys_jrn_no , prev_stmt_head_id , cash_adv_dispute_amt , rtl_pymt_rvsl_amt , cash_adv_cr_trx_amt , crcd_stmt_no , trx_count , global_trx_jrn_no , cash_adv_trx_fee_amt , cash_adv_serv_chg , overlimit_acctt_amt , crcd_cardholder_no ,  begin_rtl_bal , prev_rtl_prin , rtl_annual_fee , acct_bank_no , acct_sts , cr_trx_amt , crcd_org_no , rtl_intr_amt , rtl_cr_trx_amt , cycle_due , biz_upd_ts , prev_stmt_date , grace_date , trx_time , ctd_stmt_stat_cycle , cash_adv_cr_trx_count , upd_tlr_no , ver_no , create_tlr_no , acc_cash_bal ) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE crcd_acct_no=VALUES(crcd_acct_no), stmt_date=VALUES(stmt_date), acct_seq_no=VALUES(acct_seq_no), stmt_head_id=VALUES(stmt_head_id)"
		// values = append(values, generateValuesForTable()...)
		values = append(values, generatePrimaryValuesForTable()...)
		values = append(values, generateNonPrimaryValuesForTable()...)
	case 1: // acct_interest_log_hist
		sql = "insert into acct_interest_log_hist (crcd_acct_no , create_date , id , global_trx_jrn_no , perdiem , trx_sub_acct_no , carry_intr_day , fund_type , data_version , bal_sts , create_ts , this_times_acc_bal , acc_intr_acr_days , intr_acc_cut_off_date , crcd_cardholder_no , create_tlr_org_no , upd_ts , pt_id , upd_tlr_org_no , data_pos , trx_type , biz_date , del_flag , upd_prev_acc_fin_chg , local_id , binlog_insert_ts , crcd_org_no , stmt_date , trx_date , biz_upd_ts , cur_intr_rate , local_sys_jrn_no , src_cnsmr_sys_id , part_no , acct_seq_no , once_waive_intr_flag , migr_flag , trx_time , acct_ccy , upd_after_acc_fin_chg , upd_tlr_no , biz_scene_encod , ver_no , create_tlr_no , trx_sub_acct_type ) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE crcd_acct_no=VALUES(crcd_acct_no), create_date=VALUES(create_date), id=VALUES(id)"
		values = append(values, generatePrimaryValuesForLogTable()...)
		values = append(values, generateNonPrimaryValuesForLogTable()...)
	default:
		panic("unknown table")
	}
	return sql, values
}

func generateNonPrimaryValuesForTable() []interface{} {
	values := make([]interface{}, 0, 104)
	values = append(values, randomDecimal(13, 2)) // bp_rtl_intr_amt
	values = append(values, randomDate())         // late_chg_coll_date
	values = append(values, randomDecimal(13, 2)) // delq_total_amt
	values = append(values, randomDecimal(13, 2)) // prev_dir_to_cust_rtl_prin
	values = append(values, randomString(5))      // acct_branch_no
	values = append(values, randomDatetime(6))    // create_ts
	values = append(values, randomInt())          // agent_bank_no
	values = append(values, randomInt())          // stmt_cycle_days
	values = append(values, randomInt())          // cash_adv_days_with_bal
	values = append(values, randomDecimal(13, 2)) //acct_avail_rtl_limit
	values = append(values, randomDecimal(13, 2)) // avail_cash_adv_limit
	values = append(values, randomInt())          // dispute_trx_count
	values = append(values, randomDecimal(8, 7))  // rtl_intr_rate
	values = append(values, randomString(1))      // only_stmt_head_record_flag
	values = append(values, randomDecimal(13, 2)) // total_amt_due
	values = append(values, randomString(10))     // upd_tlr_org_no
	values = append(values, randomDecimal(13, 2)) // cash_adv_pymt_amt
	values = append(values, randomString(3))      // acct_cur_block_code
	values = append(values, randomString(1))      // stmt_mail_flag
	values = append(values, randomBigInt())       // local_id
	values = append(values, randomString(1))      // debit_cr_out_of_bal_flag
	values = append(values, randomDatetime(6))    // binlog_insert_ts
	values = append(values, randomString(3))      // acct_prev_block_code
	values = append(values, randomString(4))      // src_cnsmr_sys_id
	values = append(values, randomInt())          // part_no)
	values = append(values, randomString(1))      // dd_flag
	values = append(values, randomString(1))      // migr_flag
	values = append(values, randomString(3))      // acct_ccy
	values = append(values, randomInt())          // rtl_debit_trx_count
	values = append(values, randomDecimal(13, 2)) // intr_amt
	values = append(values, randomDecimal(13, 2)) // stmt_cash_bal
	values = append(values, randomString(1))      // write_off_sts_flag
	values = append(values, randomDecimal(13, 2)) // prev_stmt_rmn_intr
	values = append(values, randomString(10))     // create_tlr_org_no
	values = append(values, randomDatetime(6))    // upd_ts
	values = append(values, randomDecimal(13, 2)) // debit_trx_amt
	values = append(values, randomDecimal(13, 2)) // cash_adv_intr_amt
	values = append(values, randomDecimal(13, 2)) // acc_rtl_bal
	values = append(values, randomDecimal(13, 0)) // acct_rtl_limit
	values = append(values, randomDecimal(13, 2)) // cash_adv_begin_bal
	values = append(values, randomDate())         // biz_date
	values = append(values, randomDecimal(13, 2)) // late_chg
	values = append(values, randomDecimal(13, 2)) // rtl_serv_chg
	values = append(values, randomString(1))      // stmt_type
	values = append(values, randomDecimal(8, 7))  // cash_bal_1_cur_intr_rate
	values = append(values, randomDecimal(13, 2)) // begin_bal
	values = append(values, randomDecimal(13, 2)) // cur_stmt_bal
	values = append(values, randomSmallInt())     // rtl_days_with_bal
	values = append(values, randomDecimal(13, 2)) // cash_adv_debit_trx_amt
	values = append(values, randomDecimal(13, 2)) // cash_adv_amt
	values = append(values, randomDate())         // repay_expire_date
	values = append(values, randomInt())          // rtl_cr_trx_count
	values = append(values, randomString(10))     // biz_scene_encod
	values = append(values, randomDecimal(13, 2)) // rtl_dispute_amt
	values = append(values, randomDecimal(13, 2)) // last_term_stmt_forbid_instalt_prin_amt
	values = append(values, randomDecimal(13, 2)) // cash_adv_limit
	values = append(values, randomDecimal(13, 2)) // monthly_pymt
	values = append(values, randomDecimal(13, 2)) // rtl_trx_fee_amt
	values = append(values, randomString(256))    // data_version
	values = append(values, randomDecimal(13, 2)) // rtl_mis_fee
	values = append(values, randomDecimal(13, 2)) // dispute_bal
	values = append(values, randomDecimal(13, 2)) // ctd_cash_adv_pymt_rvsl
	values = append(values, randomString(10))     // data_pos
	values = append(values, randomDecimal(13, 2)) // rtl_debit_trx_amt
	values = append(values, randomDecimal(13, 2)) // stmt_rtl_bal
	values = append(values, randomDecimal(13, 2)) // last_term_spec_whls_csm_prin
	values = append(values, randomString(1))      // del_flag
	values = append(values, randomString(1))      // acct_chg_off_sts
	values = append(values, randomInt())          // cash_adv_debit_trx_count
	values = append(values, randomDecimal(13, 2)) // bp_cash_adv_intr_amt
	values = append(values, randomDecimal(13, 2)) // rtl_pymt_amt
	values = append(values, randomDate())         // trx_date
	values = append(values, randomDecimal(13, 2)) // cur_pymt_amt
	values = append(values, randomString(32))     // local_sys_jrn_no
	values = append(values, randomBigInt())       // prev_stmt_head_id
	values = append(values, randomDecimal(13, 2)) // cash_adv_dispute_amt
	values = append(values, randomDecimal(13, 2)) // rtl_pymt_rvsl_amt
	values = append(values, randomDecimal(13, 2)) // cash_adv_cr_trx_amt
	values = append(values, randomSmallInt())     // crcd_stmt_no
	values = append(values, randomInt())          // trx_count
	values = append(values, randomString(37))     // global_trx_jrn_no
	values = append(values, randomDecimal(13, 2)) // cash_adv_trx_fee_amt
	values = append(values, randomDecimal(13, 2)) // cash_adv_serv_chg
	values = append(values, randomDecimal(13, 2)) // overlimit_acctt_amt
	values = append(values, randomString(16))     // crcd_cardholder_no
	values = append(values, randomDecimal(13, 2)) // begin_rtl_bal
	values = append(values, randomDecimal(13, 2)) // prev_rtl_prin
	values = append(values, randomDecimal(13, 2)) // rtl_annual_fee
	values = append(values, randomString(4))      // acct_bank_no
	values = append(values, randomString(1))      // acct_sts
	values = append(values, randomDecimal(13, 2)) // cr_trx_amt
	values = append(values, randomString(3))      // crcd_org_no
	values = append(values, randomDecimal(13, 2)) // rtl_intr_amt
	values = append(values, randomDecimal(13, 2)) // rtl_cr_trx_amt
	values = append(values, randomInt())          // cycle_due
	values = append(values, randomDatetime(6))    // biz_upd_ts
	values = append(values, randomDate())         // prev_stmt_date
	values = append(values, randomDate())         // grace_date
	values = append(values, randomTime())         // trx_time
	values = append(values, randomInt())          // ctd_stmt_stat_cycle
	values = append(values, randomInt())          // cash_adv_cr_trx_count
	values = append(values, randomString(32))     // upd_tlr_no
	values = append(values, randomInt())          // ver_no
	values = append(values, randomString(32))     // create_tlr_no
	values = append(values, randomDecimal(13, 2)) // acc_cash_bal

	return values
}

func generatePrimaryValuesForTable() []interface{} {
	values := valuesPool.Get().([]interface{})
	defer valuesPool.Put(values[:0])

	values = append(values, randomString(16)) // crcd_acct_no
	values = append(values, randomDate())     // stmt_date
	values = append(values, randomString(3))  // acct_seq_no
	values = append(values, randomBigInt())   // stmt_head_id
	return values
}

func generatePrimaryValuesForLogTable() []interface{} {
	values := make([]interface{}, 0, 3)
	values = append(values, randomString(16)) // crcd_acct_no
	values = append(values, randomDate())     // create_date
	values = append(values, randomBigInt())   // id
	return values
}

func generateNonPrimaryValuesForLogTable() []interface{} {
	// `crcd_acct_no`,`create_date`,`id` is primary key
	values := make([]interface{}, 0, 42)
	values = append(values, randomString(37))     // global_trx_jrn_no
	values = append(values, randomDecimal(13, 6)) // perdiem
	values = append(values, randomBigInt())       // trx_sub_acct_no
	values = append(values, randomDate())         // carry_intr_day
	values = append(values, randomString(4))      // fund_type
	values = append(values, randomString(256))    // data_version
	values = append(values, randomString(1))      // bal_sts
	values = append(values, randomDatetime(6))    // create_ts
	values = append(values, randomDecimal(13, 2)) // this_times_acc_bal
	values = append(values, randomInt())          // acc_intr_acr_days
	values = append(values, randomDate())         // intr_acc_cut_off_date
	values = append(values, randomString(16))     // crcd_cardholder_no
	values = append(values, randomString(10))     // create_tlr_org_no
	values = append(values, randomDatetime(6))    // upd_ts
	values = append(values, randomString(6))      // pt_id
	values = append(values, randomString(10))     // upd_tlr_org_no
	values = append(values, randomString(10))     // data_pos
	values = append(values, randomString(4))      // trx_type
	values = append(values, randomDate())         // biz_date
	values = append(values, randomString(1))      // del_flag
	values = append(values, randomDecimal(13, 6)) // upd_prev_acc_fin_chg
	values = append(values, randomBigInt())       // local_id
	values = append(values, randomDatetime(6))    // binlog_insert_ts
	values = append(values, randomString(3))      // crcd_org_no
	values = append(values, randomDate())         // stmt_date
	values = append(values, randomDate())         // trx_date
	values = append(values, randomDatetime(6))    // biz_upd_ts
	values = append(values, randomDecimal(8, 7))  // cur_intr_rate
	values = append(values, randomString(32))     // local_sys_jrn_no
	values = append(values, randomString(4))      // src_cnsmr_sys_id
	values = append(values, randomInt())          // part_no
	values = append(values, randomString(3))      // acct_seq_no
	values = append(values, randomString(1))      // once_waive_intr_flag
	values = append(values, randomString(1))      // migr_flag
	values = append(values, randomTime())         // trx_time
	values = append(values, randomString(3))      // acct_ccy
	values = append(values, randomDecimal(13, 6)) // upd_after_acc_fin_chg
	values = append(values, randomString(32))     // upd_tlr_no
	values = append(values, randomString(10))     // biz_scene_encod
	values = append(values, randomInt())          // ver_no
	values = append(values, randomString(32))     // create_tlr_no
	values = append(values, randomString(6))      // trx_sub_acct_type

	return values
}

func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func randomDate() string {
	min := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Now().Unix()
	delta := max - min
	sec := rand.Int63n(delta) + min
	return time.Unix(sec, 0).Format("2006-01-02")
}

func randomBigInt() int64 {
	min := int64(0)
	max := int64(1<<63 - 1)
	return rand.Int63n(max-min) + min
}

func randomInt() int32 {
	min := int32(0)
	max := int32(1<<31 - 1)
	return rand.Int31n(max-min) + min
}

func randomSmallInt() int16 {
	raw := rand.Int31n(65536)
	smallintValue := int16(raw - 32768)
	return int16(smallintValue)
}

func randomDecimal(precision, scale int) string {
	integerDigits := precision - scale
	maxInteger := int64(1)
	for i := 0; i < integerDigits; i++ {
		maxInteger *= 10
	}
	integerPart := rand.Int63n(maxInteger)

	maxFraction := int64(1)
	for i := 0; i < scale; i++ {
		maxFraction *= 10
	}
	fractionPart := rand.Int63n(maxFraction)

	decimalValue := fmt.Sprintf("%d.%0*d", integerPart, scale, fractionPart)
	return decimalValue
}

func randomDatetime(precision int) string {
	min := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Now().Unix()
	delta := max - min
	sec := rand.Int63n(delta) + min

	format := "2006-01-02 15:04:05"
	switch precision {
	case 1:
		format += ".0"
	case 2:
		format += ".00"
	case 3:
		format += ".000"
	case 4:
		format += ".0000"
	case 5:
		format += ".00000"
	case 6:
		format += ".000000"
	default:
		format += ".000000"
	}

	return time.Unix(sec, 0).Format(format)
}

func randomTime() string {
	start := time.Now().AddDate(-40, 0, 0)
	end := time.Now()
	delta := end.Sub(start)
	randomDuration := time.Duration(rand.Int63n(int64(delta)))
	return start.Add(randomDuration).Format("15:04:05")
}
