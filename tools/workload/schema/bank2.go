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
	local_id bigint(20) NOT NULL COMMENT '本地id',
	stmt_head_id bigint(20) NOT NULL COMMENT '账单头ID',
	stmt_date date NOT NULL COMMENT '账单日期',
	part_no int(11) NOT NULL DEFAULT '0' COMMENT '分片编号',
	crcd_acct_no varchar(16) NOT NULL DEFAULT '' COMMENT '信用卡账户号',
	acct_seq_no varchar(3) NOT NULL DEFAULT '' COMMENT '账户序号',
	acct_ccy varchar(3) NOT NULL DEFAULT '' COMMENT '账户币种: 156-156-人民币元, 344-344-香港元, 840-840-美元',
	crcd_org_no varchar(3) NOT NULL DEFAULT '' COMMENT '信用卡机构号',
	crcd_cardholder_no varchar(16) NOT NULL DEFAULT '' COMMENT '信用卡持卡人号',
	prev_stmt_head_id bigint(20) NOT NULL DEFAULT '0' COMMENT '上一账单头id',
	repay_expire_date date DEFAULT NULL COMMENT '还款到期日期',
	late_chg_coll_date date DEFAULT NULL COMMENT '违约金收取日期',
	grace_date date DEFAULT NULL COMMENT '宽限日期',
	total_amt_due decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '最低还款额',
	monthly_pymt decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '当期最低应缴',
	delq_total_amt decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '延滞总金额',
	overlimit_acctt_amt decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '超限记账金额',
	cur_pymt_amt decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '本期还款金额',
	begin_bal decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '上期账单金额',
	debit_trx_amt decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '借记交易金额',
	cr_trx_amt decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '贷记交易金额',
	dispute_bal decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '争议余额',
	dispute_trx_count int(11) NOT NULL DEFAULT '0' COMMENT '争议交易笔数',
	crcd_stmt_no smallint(6) NOT NULL COMMENT '信用卡账单号',
	acct_rtl_limit decimal(13,0) NOT NULL DEFAULT '0' COMMENT '账户消费额度',
	acct_avail_rtl_limit decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '账户可用消费额度',
	stmt_cycle_days int(11) NOT NULL DEFAULT '0' COMMENT '账单周期内天数',
	cash_adv_amt decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '取现金额',
	intr_amt decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '利息金额',
	stmt_type varchar(1) NOT NULL DEFAULT '' COMMENT '账单类型: 1-组合对账单, 2-定期生成对帐单－不寄出, 3-组合对账单－不寄出, 更多请在dama查看',
	debit_cr_out_of_bal_flag varchar(1) NOT NULL DEFAULT '' COMMENT '借贷统计金额不平标识: N-平账, Y-不平账',
	acct_prev_block_code varchar(3) NOT NULL DEFAULT '' COMMENT '账户上一封锁码',
	stmt_mail_flag varchar(1) NOT NULL DEFAULT '' COMMENT '账单邮寄标识',
	only_stmt_head_record_flag varchar(1) NOT NULL DEFAULT '' COMMENT '只有账单头记录标识',
	bp_rtl_intr_amt decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '账户细分余额消费利息金额',
	bp_cash_adv_intr_amt decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '账户细分余额取现利息金额',
	acct_sts varchar(1) NOT NULL DEFAULT '' COMMENT '账户状态: 0-0-新账户, 1-1-活跃账户, 2-2-非活跃账户, 3-3-转卡, 4-4-卡转, 更多请在dama查看',
	acct_cur_block_code varchar(3) NOT NULL DEFAULT '' COMMENT '账户当前封锁码',
	cycle_due int(11) NOT NULL DEFAULT '0' COMMENT '延滞期数',
	trx_count int(11) NOT NULL DEFAULT '0' COMMENT '交易笔数',
	begin_rtl_bal decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '期初消费余额',
	acc_rtl_bal decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '累计消费余额',
	rtl_days_with_bal smallint(6) NOT NULL DEFAULT '0' COMMENT '消费有余额日数',
	rtl_pymt_amt decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '消费还款金额',
	rtl_pymt_rvsl_amt decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '消费还款还原金额',
	rtl_debit_trx_amt decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '消费借记交易金额',
	rtl_debit_trx_count int(11) NOT NULL DEFAULT '0' COMMENT '消费借记交易笔数',
	rtl_cr_trx_amt decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '消费贷记交易金额',
	rtl_cr_trx_count int(11) NOT NULL DEFAULT '0' COMMENT '消费贷记交易笔数',
	rtl_serv_chg decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '消费服务费',
	rtl_trx_fee_amt decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '消费交易费金额',
	rtl_intr_amt decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '消费利息金额',
	rtl_mis_fee decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '消费杂费',
	rtl_annual_fee decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '消费年费',
	stmt_rtl_bal decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '账单消费余额',
	rtl_dispute_amt decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '消费争议金额',
	cash_adv_begin_bal decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '提现期初余额',
	acc_cash_bal decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '累计取现余额',
	cash_adv_days_with_bal int(11) NOT NULL DEFAULT '0' COMMENT '取现有余额日数',
	cash_adv_pymt_amt decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '取现还款金额',
	ctd_cash_adv_pymt_rvsl decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '当期取现还款还原',
	cash_adv_debit_trx_amt decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '取现借记交易金额',
	cash_adv_debit_trx_count int(11) NOT NULL DEFAULT '0' COMMENT '取现借记交易笔数',
	cash_adv_cr_trx_amt decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '取现贷记交易金额',
	cash_adv_cr_trx_count int(11) NOT NULL DEFAULT '0' COMMENT '取现贷记交易笔数',
	cash_adv_serv_chg decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '提现透支手续费',
	cash_adv_trx_fee_amt decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '取现交易费金额',
	cash_adv_intr_amt decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '取现利息金额',
	stmt_cash_bal decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '账单取现余额',
	cash_adv_dispute_amt decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '取现争端交易金额',
	cash_adv_limit decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '取现额度',
	avail_cash_adv_limit decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '可用取现额度',
	agent_bank_no int(11) NOT NULL DEFAULT '0' COMMENT '代理银行号',
	dd_flag varchar(1) NOT NULL DEFAULT '' COMMENT '自动还款标识: 0-无自动还款, 1-自动还款, 2-已授权自动还款',
	prev_stmt_rmn_intr decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '上期账单未还利息',
	write_off_sts_flag varchar(1) NOT NULL DEFAULT '' COMMENT '核销状态标识: 0-正常, 1-人工冻结进行中, 2-人工冻结完成, 4-自动冻结进行中, 5-自动冻结完成, 更多请在dama查看',
	prev_rtl_prin decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '上期消费本金',
	cur_stmt_bal decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '当前账单余额',
	ctd_stmt_stat_cycle int(11) NOT NULL DEFAULT '0' COMMENT '当期账单统计期数',
	prev_stmt_date date DEFAULT NULL COMMENT '上期账单日期',
	rtl_intr_rate decimal(8,7) NOT NULL DEFAULT '0.0000000' COMMENT '消费利率',
	late_chg decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '违约金',
	cash_bal_1_cur_intr_rate decimal(8,7) NOT NULL DEFAULT '0.0000000' COMMENT '取现余额1当前利率',
	global_trx_jrn_no varchar(37) DEFAULT NULL COMMENT '全局交易流水号',
	src_cnsmr_sys_id varchar(4) DEFAULT NULL COMMENT '源消费方系统id',
	local_sys_jrn_no varchar(32) DEFAULT NULL COMMENT '本系统流水号',
	biz_date date DEFAULT NULL COMMENT '业务日期',
	trx_date date DEFAULT NULL COMMENT '交易日期',
	trx_time time DEFAULT NULL COMMENT '交易时间',
	biz_scene_encod varchar(10) DEFAULT NULL COMMENT '业务场景编码',
	data_pos varchar(10) NOT NULL DEFAULT '' COMMENT '数据位置',
	create_tlr_no varchar(32) NOT NULL DEFAULT '' COMMENT '创建柜员号',
	upd_tlr_no varchar(32) NOT NULL DEFAULT '' COMMENT '更新柜员号',
	create_tlr_org_no varchar(10) NOT NULL DEFAULT '' COMMENT '创建柜员机构号',
	upd_tlr_org_no varchar(10) NOT NULL DEFAULT '' COMMENT '更新柜员机构号',
	biz_upd_ts datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '业务更新时间戳',
	ver_no int(11) NOT NULL DEFAULT '0' COMMENT '版本编号',
	create_ts datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '创建时间戳',
	upd_ts datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新时间戳',
	acct_bank_no varchar(4) NOT NULL DEFAULT '' COMMENT '账户分行号',
	acct_branch_no varchar(5) NOT NULL DEFAULT '' COMMENT '账户支行号',
	migr_flag varchar(1) NOT NULL DEFAULT '0' COMMENT '迁移标识',
	acct_chg_off_sts varchar(1) NOT NULL DEFAULT '' COMMENT '账户冻结状态: 0-0-正常账户, 1-1-等待人工冻结, 2-2-人工冻结, 4-4-等待自动冻结, 更多请在dama查看',
	binlog_insert_ts datetime(6) NOT NULL DEFAULT '1900-01-01 00:00:00.000000' COMMENT 'flink同步数据写入sdb的系统时间，特殊值说明：\n      主机核心迁移:1900-01-01 00:00:00.000000,\n      在线查询平台迁移:1901-01-0100:00:00.000000,\n      准实时修数：1999-01-01 00:00:00.000000',
	del_flag varchar(1) NOT NULL DEFAULT 'N' COMMENT '删除状态: N-正常;Y-已删除',
	data_version varchar(256) DEFAULT NULL COMMENT '同步技术字段，数据版本(kafka sink end time)',
	prev_dir_to_cust_rtl_prin decimal(13,2) DEFAULT NULL COMMENT '上期直接对客消费本金',
	last_term_spec_whls_csm_prin decimal(13,2) DEFAULT NULL COMMENT '上期专项大额消费本金',
	last_term_stmt_forbid_instalt_prin_amt decimal(13,2) DEFAULT NULL,
	PRIMARY KEY (crcd_acct_no,stmt_date,acct_seq_no,stmt_head_id) /*T![clustered_index] CLUSTERED */,
	KEY upd_ts_del_flag_idx (upd_ts,data_pos,del_flag),
	KEY acct_statement_head_info_idx_1 (crcd_cardholder_no,stmt_date),
	KEY acct_statement_head_info_idx_2 (stmt_head_id,stmt_date)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='账单头信息表'`

const createLogTableSQL = `CREATE TABLE acct_interest_log_hist (
  id bigint(20) NOT NULL COMMENT 'id',
  local_id bigint(20) NOT NULL COMMENT '本地id',
  create_date date NOT NULL COMMENT '创建日期',
  part_no int(11) NOT NULL DEFAULT '0' COMMENT '分片编号',
  crcd_acct_no varchar(16) NOT NULL DEFAULT '' COMMENT '信用卡账户号',
  acct_seq_no varchar(3) NOT NULL DEFAULT '' COMMENT '账户序号',
  acct_ccy varchar(3) NOT NULL DEFAULT '' COMMENT '账户币种: 156-156-人民币元, 344-344-香港元, 840-840-美元',
  crcd_org_no varchar(3) NOT NULL DEFAULT '' COMMENT '信用卡机构号',
  crcd_cardholder_no varchar(16) NOT NULL DEFAULT '' COMMENT '信用卡持卡人号',
  trx_sub_acct_no bigint(20) NOT NULL DEFAULT '0' COMMENT '交易子账户编号',
  trx_type varchar(4) NOT NULL DEFAULT '' COMMENT '交易类型: INSQ-分期, INSR-分期还原, REVR-撤销冲正',
  intr_acc_cut_off_date date DEFAULT NULL COMMENT '利息累计截止日期',
  this_times_acc_bal decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '本次累积余额',
  acc_intr_acr_days int(11) NOT NULL DEFAULT '0' COMMENT '累计计息天数',
  cur_intr_rate decimal(8,7) NOT NULL DEFAULT '0.0000000' COMMENT '当前利率',
  pt_id varchar(6) NOT NULL DEFAULT '' COMMENT '利率表ID',
  upd_prev_acc_fin_chg decimal(13,6) NOT NULL DEFAULT '0.000000' COMMENT '更新前累积利息金额',
  upd_after_acc_fin_chg decimal(13,6) NOT NULL DEFAULT '0.000000' COMMENT '更新后累积利息金额',
  carry_intr_day date DEFAULT NULL COMMENT '起息日',
  perdiem decimal(13,6) NOT NULL DEFAULT '0.000000' COMMENT '每日利息',
  trx_sub_acct_type varchar(6) NOT NULL DEFAULT '' COMMENT '交易子账户类型',
  stmt_date date DEFAULT NULL COMMENT '账单日期',
  global_trx_jrn_no varchar(37) DEFAULT NULL COMMENT '全局交易流水号',
  src_cnsmr_sys_id varchar(4) DEFAULT NULL COMMENT '源消费方系统id',
  local_sys_jrn_no varchar(32) DEFAULT NULL COMMENT '本系统流水号',
  biz_date date NOT NULL COMMENT '业务日期',
  trx_date date DEFAULT NULL COMMENT '交易日期',
  trx_time time DEFAULT NULL COMMENT '交易时间',
  biz_scene_encod varchar(10) NOT NULL DEFAULT '' COMMENT '业务场景编码',
  data_pos varchar(10) NOT NULL DEFAULT '' COMMENT '数据位置',
  create_tlr_no varchar(32) NOT NULL DEFAULT '' COMMENT '创建柜员号',
  upd_tlr_no varchar(32) NOT NULL DEFAULT '' COMMENT '更新柜员号',
  create_tlr_org_no varchar(10) NOT NULL DEFAULT '' COMMENT '创建柜员机构号',
  upd_tlr_org_no varchar(10) NOT NULL DEFAULT '' COMMENT '更新柜员机构号',
  biz_upd_ts datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '业务更新时间戳',
  ver_no int(11) NOT NULL DEFAULT '0' COMMENT '版本编号',
  create_ts datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '创建时间戳',
  upd_ts datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新时间戳',
  fund_type varchar(4) NOT NULL DEFAULT '' COMMENT '资金类型',
  migr_flag varchar(1) NOT NULL DEFAULT '0' COMMENT '迁移标识',
  bal_sts varchar(1) NOT NULL DEFAULT '' COMMENT '余额状态',
  once_waive_intr_flag varchar(1) NOT NULL DEFAULT '' COMMENT '一次免息标识',
  binlog_insert_ts datetime(6) NOT NULL DEFAULT '1900-01-01 00:00:00.000000' COMMENT 'flink同步数据写入sdb的系统时间，特殊值说明：\n      主机核心迁移:1900-01-01 00:00:00.000',
  del_flag varchar(1) NOT NULL DEFAULT 'N' COMMENT '删除状态: N-正常;Y-已删除',
  data_version varchar(256) DEFAULT NULL COMMENT '同步技术字段，数据版本(kafka sink end time)',
  PRIMARY KEY (crcd_acct_no,create_date,id) /*T![clustered_index] NONCLUSTERED */,
  KEY acct_interest_log_hist_idx_1 (crcd_acct_no,stmt_date,id),
  KEY upd_ts_del_flag_idx (upd_ts,data_pos,del_flag)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T! SHARD_ROW_ID_BITS=9 PRE_SPLIT_REGIONS=9 */ COMMENT='利息累计历史表'`

type Bank2Workload struct {
	infoTableInsertSQL string
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
	return &Bank2Workload{infoTableInsertSQL: builder.String()}
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

func (c *Bank2Workload) BuildInsertSqlWithValues(tableN int, batchSize int, channel chan SqlValue) {
	nonPrimaryKeyValues := generateNonPrimaryValuesForTable() // to reduce time, these field we keep same for
	sql := c.infoTableInsertSQL

	switch tableN {
	case 0: // acct_statement_head_info

		for {
			rand.Seed(time.Now().UnixNano())

			values := valuesPool.Get().([]interface{})
			defer valuesPool.Put(values[:0]) // 使用后重置并放回池中
			// 200 rows one txn
			for r := 0; r < 200; r++ {
				values = append(values, generatePrimaryValuesForTable()...)
				values = append(values, nonPrimaryKeyValues...)
			}

			channel <- SqlValue{Sql: sql, Values: values}
		}
		// for i < batchSize {
		// 	i += 1
		// builder.WriteString("insert into acct_statement_head_info (crcd_acct_no , stmt_date , acct_seq_no , stmt_head_id ,bp_rtl_intr_amt , late_chg_coll_date , delq_total_amt , prev_dir_to_cust_rtl_prin , acct_branch_no , create_ts , agent_bank_no , stmt_cycle_days , cash_adv_days_with_bal , acct_avail_rtl_limit , avail_cash_adv_limit , dispute_trx_count , rtl_intr_rate , only_stmt_head_record_flag , total_amt_due , upd_tlr_org_no , cash_adv_pymt_amt , acct_cur_block_code , stmt_mail_flag , local_id , debit_cr_out_of_bal_flag , binlog_insert_ts , acct_prev_block_code , src_cnsmr_sys_id , part_no , dd_flag , migr_flag , acct_ccy , rtl_debit_trx_count , intr_amt , stmt_cash_bal , write_off_sts_flag , prev_stmt_rmn_intr , create_tlr_org_no , upd_ts , debit_trx_amt , cash_adv_intr_amt , acc_rtl_bal , acct_rtl_limit , cash_adv_begin_bal , biz_date , late_chg , rtl_serv_chg , stmt_type , cash_bal_1_cur_intr_rate , begin_bal , cur_stmt_bal , rtl_days_with_bal , cash_adv_debit_trx_amt , cash_adv_amt , repay_expire_date , rtl_cr_trx_count , biz_scene_encod , rtl_dispute_amt , last_term_stmt_forbid_instalt_prin_amt , cash_adv_limit , monthly_pymt , rtl_trx_fee_amt , data_version , rtl_mis_fee , dispute_bal , ctd_cash_adv_pymt_rvsl , data_pos , rtl_debit_trx_amt , stmt_rtl_bal , last_term_spec_whls_csm_prin , del_flag , acct_chg_off_sts , cash_adv_debit_trx_count , bp_cash_adv_intr_amt , rtl_pymt_amt , trx_date , cur_pymt_amt , local_sys_jrn_no , prev_stmt_head_id , cash_adv_dispute_amt , rtl_pymt_rvsl_amt , cash_adv_cr_trx_amt , crcd_stmt_no , trx_count , global_trx_jrn_no , cash_adv_trx_fee_amt , cash_adv_serv_chg , overlimit_acctt_amt , crcd_cardholder_no ,  begin_rtl_bal , prev_rtl_prin , rtl_annual_fee , acct_bank_no , acct_sts , cr_trx_amt , crcd_org_no , rtl_intr_amt , rtl_cr_trx_amt , cycle_due , biz_upd_ts , prev_stmt_date , grace_date , trx_time , ctd_stmt_stat_cycle , cash_adv_cr_trx_count , upd_tlr_no , ver_no , create_tlr_no , acc_cash_bal ) values (")
		// // 200 rows one txn
		// for r := 0; r < 200; r++ {
		// 	builder.WriteString(generatePrimaryValuesForTable())
		// 	builder.WriteString(nonPrimaryKeyValues)
		// 	if r+1 < 200 {
		// 		builder.WriteString("),(")
		// 	} else {
		// 		builder.WriteString(")")
		// 	}
		// }
		// builder.WriteString(";")
		// }

		//sql += builder.String()
	// case 1: // acct_interest_log_hist
	// 	builder.WriteString("insert into acct_interest_log_hist ( global_trx_jrn_no , perdiem , trx_sub_acct_no , carry_intr_day , fund_type , data_version , bal_sts , create_ts , this_times_acc_bal , acc_intr_acr_days , intr_acc_cut_off_date , crcd_cardholder_no , create_tlr_org_no , upd_ts , pt_id , upd_tlr_org_no , data_pos , trx_type , id , create_date , biz_date , del_flag , upd_prev_acc_fin_chg , local_id , binlog_insert_ts , crcd_org_no , stmt_date , trx_date , biz_upd_ts , cur_intr_rate , crcd_acct_no , local_sys_jrn_no , src_cnsmr_sys_id , part_no , acct_seq_no , once_waive_intr_flag , migr_flag , trx_time , acct_ccy , upd_after_acc_fin_chg , upd_tlr_no , biz_scene_encod , ver_no , create_tlr_no , trx_sub_acct_type ) values ")

	// 	for r := 0; r < 200; r++ {
	// 		values = append(values, generateValuesForLogTable()...)
	// 		if r != 0 {
	// 			builder.WriteString(",")
	// 		}
	// 		builder.WriteString("(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
	// 	}
	// 	sql += builder.String()
	default:
		panic("unknown table")
	}
	//return sql, values
}

func (c *Bank2Workload) BuildUpdateSqlWithValues(opts UpdateOption) (string, []interface{}) {
	var sql string
	values := make([]interface{}, 0, 120)
	/*switch opts.Table {
	case 0: // acct_statement_head_info
		sql = fmt.Sprintf("insert into acct_statement_head_info (crcd_acct_no , stmt_date , acct_seq_no , stmt_head_id ,bp_rtl_intr_amt , late_chg_coll_date , delq_total_amt , prev_dir_to_cust_rtl_prin , acct_branch_no , create_ts , agent_bank_no , stmt_cycle_days , cash_adv_days_with_bal , acct_avail_rtl_limit , avail_cash_adv_limit , dispute_trx_count , rtl_intr_rate , only_stmt_head_record_flag , total_amt_due , upd_tlr_org_no , cash_adv_pymt_amt , acct_cur_block_code , stmt_mail_flag , local_id , debit_cr_out_of_bal_flag , binlog_insert_ts , acct_prev_block_code , src_cnsmr_sys_id , part_no , dd_flag , migr_flag , acct_ccy , rtl_debit_trx_count , intr_amt , stmt_cash_bal , write_off_sts_flag , prev_stmt_rmn_intr , create_tlr_org_no , upd_ts , debit_trx_amt , cash_adv_intr_amt , acc_rtl_bal , acct_rtl_limit , cash_adv_begin_bal , biz_date , late_chg , rtl_serv_chg , stmt_type , cash_bal_1_cur_intr_rate , begin_bal , cur_stmt_bal , rtl_days_with_bal , cash_adv_debit_trx_amt , cash_adv_amt , repay_expire_date , rtl_cr_trx_count , biz_scene_encod , rtl_dispute_amt , last_term_stmt_forbid_instalt_prin_amt , cash_adv_limit , monthly_pymt , rtl_trx_fee_amt , data_version , rtl_mis_fee , dispute_bal , ctd_cash_adv_pymt_rvsl , data_pos , rtl_debit_trx_amt , stmt_rtl_bal , last_term_spec_whls_csm_prin , del_flag , acct_chg_off_sts , cash_adv_debit_trx_count , bp_cash_adv_intr_amt , rtl_pymt_amt , trx_date , cur_pymt_amt , local_sys_jrn_no , prev_stmt_head_id , cash_adv_dispute_amt , rtl_pymt_rvsl_amt , cash_adv_cr_trx_amt , crcd_stmt_no , trx_count , global_trx_jrn_no , cash_adv_trx_fee_amt , cash_adv_serv_chg , overlimit_acctt_amt , crcd_cardholder_no ,  begin_rtl_bal , prev_rtl_prin , rtl_annual_fee , acct_bank_no , acct_sts , cr_trx_amt , crcd_org_no , rtl_intr_amt , rtl_cr_trx_amt , cycle_due , biz_upd_ts , prev_stmt_date , grace_date , trx_time , ctd_stmt_stat_cycle , cash_adv_cr_trx_count , upd_tlr_no , ver_no , create_tlr_no , acc_cash_bal ) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE crcd_acct_no=VALUES(crcd_acct_no), stmt_date=VALUES(stmt_date), acct_seq_no=VALUES(acct_seq_no), stmt_head_id=VALUES(stmt_head_id)")
		values = append(values, generateValuesForTable()...)
	case 1: // acct_interest_log_hist
		sql = fmt.Sprintf("insert into acct_interest_log_hist ( global_trx_jrn_no , perdiem , trx_sub_acct_no , carry_intr_day , fund_type , data_version , bal_sts , create_ts , this_times_acc_bal , acc_intr_acr_days , intr_acc_cut_off_date , crcd_cardholder_no , create_tlr_org_no , upd_ts , pt_id , upd_tlr_org_no , data_pos , trx_type , id , create_date , biz_date , del_flag , upd_prev_acc_fin_chg , local_id , binlog_insert_ts , crcd_org_no , stmt_date , trx_date , biz_upd_ts , cur_intr_rate , crcd_acct_no , local_sys_jrn_no , src_cnsmr_sys_id , part_no , acct_seq_no , once_waive_intr_flag , migr_flag , trx_time , acct_ccy , upd_after_acc_fin_chg , upd_tlr_no , biz_scene_encod , ver_no , create_tlr_no , trx_sub_acct_type ) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE crcd_acct_no=VALUES(crcd_acct_no), create_date=VALUES(create_date), id=VALUES(id)")
		values = append(values, generateValuesForLogTable()...)
	default:
		panic("unknown table")
	}*/
	return sql, values
}

func generateNonPrimaryValuesForTable() []interface{} {
	/*var builder strings.Builder
	builder.WriteString(randomDecimal(13, 2)) // bp_rtl_intr_amt
	builder.WriteString(",'")
	builder.WriteString(randomDate()) // late_chg_coll_date
	builder.WriteString("',")
	builder.WriteString(randomDecimal(13, 2)) // delq_total_amt
	builder.WriteString(",")
	builder.WriteString(randomDecimal(13, 2)) // prev_dir_to_cust_rtl_prin
	builder.WriteString(",'")
	builder.WriteString(randomString(5)) // acct_branch_no
	builder.WriteString("','")
	builder.WriteString(randomDatetime(6)) // create_ts
	builder.WriteString("',")
	builder.WriteString(strconv.FormatInt(int64(randomInt()), 10)) // agent_bank_no
	builder.WriteString(",")
	builder.WriteString(strconv.FormatInt(int64(randomInt()), 10)) // stmt_cycle_days
	builder.WriteString(",")
	builder.WriteString(strconv.FormatInt(int64(randomInt()), 10)) // cash_adv_days_with_bal
	builder.WriteString(",")
	builder.WriteString(randomDecimal(13, 2)) //acct_avail_rtl_limit
	builder.WriteString(",")
	builder.WriteString(randomDecimal(13, 2)) // avail_cash_adv_limit
	builder.WriteString(",")
	builder.WriteString(strconv.FormatInt(int64(randomInt()), 10)) // dispute_trx_count
	builder.WriteString(",")
	builder.WriteString(randomDecimal(8, 7)) // rtl_intr_rate
	builder.WriteString(",'")
	builder.WriteString(randomString(1)) // only_stmt_head_record_flag
	builder.WriteString("',")
	builder.WriteString(randomDecimal(13, 2)) // total_amt_due
	builder.WriteString(",'")
	builder.WriteString(randomString(10)) // upd_tlr_org_no
	builder.WriteString("',")
	builder.WriteString(randomDecimal(13, 2)) // cash_adv_pymt_amt
	builder.WriteString(",'")
	builder.WriteString(randomString(3)) // acct_cur_block_code
	builder.WriteString("','")
	builder.WriteString(randomString(1)) // stmt_mail_flag
	builder.WriteString("',")
	builder.WriteString(strconv.FormatInt(int64(randomInt()), 10)) // local_id
	builder.WriteString(",'")
	builder.WriteString(randomString(1)) // debit_cr_out_of_bal_flag
	builder.WriteString("','")
	builder.WriteString(randomDatetime(6)) // binlog_insert_ts
	builder.WriteString("','")
	builder.WriteString(randomString(3)) // acct_prev_block_code
	builder.WriteString("','")
	builder.WriteString(randomString(4)) // src_cnsmr_sys_id
	builder.WriteString("',")
	builder.WriteString(strconv.FormatInt(int64(randomInt()), 10)) // part_no
	builder.WriteString(",'")
	builder.WriteString(randomString(1)) // dd_flag
	builder.WriteString("','")
	builder.WriteString(randomString(1)) // migr_flag
	builder.WriteString("','")
	builder.WriteString(randomString(3)) // acct_ccy
	builder.WriteString("',")
	builder.WriteString(strconv.FormatInt(int64(randomInt()), 10)) // rtl_debit_trx_count
	builder.WriteString(",")
	builder.WriteString(randomDecimal(13, 2)) // intr_amt
	builder.WriteString(",")
	builder.WriteString(randomDecimal(13, 2)) // stmt_cash_bal
	builder.WriteString(",'")
	builder.WriteString(randomString(1)) // write_off_sts_flag
	builder.WriteString("',")
	builder.WriteString(randomDecimal(13, 2)) // prev_stmt_rmn_intr
	builder.WriteString(",'")
	builder.WriteString(randomString(10)) // create_tlr_org_no
	builder.WriteString("','")
	builder.WriteString(randomDatetime(6)) // upd_ts
	builder.WriteString("',")
	builder.WriteString(randomDecimal(13, 2)) // debit_trx_amt
	builder.WriteString(",")
	builder.WriteString(randomDecimal(13, 2)) // cash_adv_intr_amt
	builder.WriteString(",")
	builder.WriteString(randomDecimal(13, 2)) // acc_rtl_bal
	builder.WriteString(",")
	builder.WriteString(randomDecimal(13, 0)) // acct_rtl_limit
	builder.WriteString(",")
	builder.WriteString(randomDecimal(13, 2)) // cash_adv_begin_bal
	builder.WriteString(",'")
	builder.WriteString(randomDate()) // biz_date
	builder.WriteString("',")
	builder.WriteString(randomDecimal(13, 2)) // late_chg
	builder.WriteString(",")
	builder.WriteString(randomDecimal(13, 2)) // rtl_serv_chg
	builder.WriteString(",'")
	builder.WriteString(randomString(1)) // stmt_type
	builder.WriteString("',")
	builder.WriteString(randomDecimal(8, 7)) // cash_bal_1_cur_intr_rate
	builder.WriteString(",")
	builder.WriteString(randomDecimal(13, 2)) // begin_bal
	builder.WriteString(",")
	builder.WriteString(randomDecimal(13, 2)) // cur_stmt_bal
	builder.WriteString(",")
	builder.WriteString(strconv.FormatInt(int64(randomSmallInt()), 10)) //rtl_days_with_bal
	builder.WriteString(",")
	builder.WriteString(randomDecimal(13, 2)) // cash_adv_debit_trx_amt
	builder.WriteString(",")
	builder.WriteString(randomDecimal(13, 2)) // cash_adv_amt
	builder.WriteString(",'")
	builder.WriteString(randomDate()) // repay_expire_date
	builder.WriteString("',")
	builder.WriteString(strconv.FormatInt(int64(randomInt()), 10)) //rtl_cr_trx_count
	builder.WriteString(",'")
	builder.WriteString(randomString(10)) // biz_scene_encod
	builder.WriteString("',")
	builder.WriteString(randomDecimal(13, 2)) // rtl_dispute_amt
	builder.WriteString(",")
	builder.WriteString(randomDecimal(13, 2)) // last_term_stmt_forbid_instalt_prin_amt
	builder.WriteString(",")
	builder.WriteString(randomDecimal(13, 2)) // cash_adv_limit
	builder.WriteString(",")
	builder.WriteString(randomDecimal(13, 2)) // monthly_pymt
	builder.WriteString(",")
	builder.WriteString(randomDecimal(13, 2)) // rtl_trx_fee_amt
	builder.WriteString(",'")
	builder.WriteString(randomString(256)) // data_version
	builder.WriteString("',")
	builder.WriteString(randomDecimal(13, 2)) // rtl_mis_fee
	builder.WriteString(",")
	builder.WriteString(randomDecimal(13, 2)) // dispute_bal
	builder.WriteString(",")
	builder.WriteString(randomDecimal(13, 2)) // ctd_cash_adv_pymt_rvsl
	builder.WriteString(",'")
	builder.WriteString(randomString(10)) // data_pos
	builder.WriteString("',")
	builder.WriteString(randomDecimal(13, 2)) // rtl_debit_trx_amt
	builder.WriteString(",")
	builder.WriteString(randomDecimal(13, 2)) // stmt_rtl_bal
	builder.WriteString(",")
	builder.WriteString(randomDecimal(13, 2)) // last_term_spec_whls_csm_prin
	builder.WriteString(",'")
	builder.WriteString(randomString(1)) // del_flag
	builder.WriteString("','")
	builder.WriteString(randomString(1)) // acct_chg_off_sts
	builder.WriteString("',")
	builder.WriteString(strconv.FormatInt(int64(randomInt()), 10)) //cash_adv_debit_trx_count
	builder.WriteString(",")
	builder.WriteString(randomDecimal(13, 2)) // bp_cash_adv_intr_amt
	builder.WriteString(",")
	builder.WriteString(randomDecimal(13, 2)) // rtl_pymt_amt
	builder.WriteString(",'")
	builder.WriteString(randomDate()) // trx_date
	builder.WriteString("',")
	builder.WriteString(randomDecimal(13, 2)) // cur_pymt_amt
	builder.WriteString(",'")
	builder.WriteString(randomString(32)) // local_sys_jrn_no
	builder.WriteString("',")
	builder.WriteString(strconv.FormatInt(randomBigInt(), 10)) //cash_adv_debit_trx_count
	builder.WriteString(",")
	builder.WriteString(randomDecimal(13, 2)) // cash_adv_dispute_amt
	builder.WriteString(",")
	builder.WriteString(randomDecimal(13, 2)) // rtl_pymt_rvsl_amt
	builder.WriteString(",")
	builder.WriteString(randomDecimal(13, 2)) // cash_adv_cr_trx_amt
	builder.WriteString(",")
	builder.WriteString(strconv.FormatInt(int64(randomSmallInt()), 10)) //crcd_stmt_no
	builder.WriteString(",")
	builder.WriteString(strconv.FormatInt(int64(randomInt()), 10)) //trx_count
	builder.WriteString(",'")
	builder.WriteString(randomString(37)) // global_trx_jrn_no
	builder.WriteString("',")
	builder.WriteString(randomDecimal(13, 2)) // cash_adv_trx_fee_amt
	builder.WriteString(",")
	builder.WriteString(randomDecimal(13, 2)) // cash_adv_serv_chg
	builder.WriteString(",")
	builder.WriteString(randomDecimal(13, 2)) // overlimit_acctt_amt
	builder.WriteString(",'")
	builder.WriteString(randomString(16)) // crcd_cardholder_no
	builder.WriteString("',")
	builder.WriteString(randomDecimal(13, 2)) // begin_rtl_bal
	builder.WriteString(",")
	builder.WriteString(randomDecimal(13, 2)) // prev_rtl_prin
	builder.WriteString(",")
	builder.WriteString(randomDecimal(13, 2)) // rtl_annual_fee
	builder.WriteString(",'")
	builder.WriteString(randomString(4)) // acct_bank_no
	builder.WriteString("','")
	builder.WriteString(randomString(1)) // acct_sts
	builder.WriteString("',")
	builder.WriteString(randomDecimal(13, 2)) // cr_trx_amt
	builder.WriteString(",'")
	builder.WriteString(randomString(3)) // crcd_org_no
	builder.WriteString("',")
	builder.WriteString(randomDecimal(13, 2)) // rtl_intr_amt
	builder.WriteString(",")
	builder.WriteString(randomDecimal(13, 2)) // rtl_cr_trx_amt
	builder.WriteString(",")
	builder.WriteString(strconv.FormatInt(int64(randomInt()), 10)) //cycle_due
	builder.WriteString(",'")
	builder.WriteString(randomDatetime(6)) // biz_upd_ts
	builder.WriteString("','")
	builder.WriteString(randomDate()) // prev_stmt_date
	builder.WriteString("','")
	builder.WriteString(randomDate()) // grace_date
	builder.WriteString("','")
	builder.WriteString(randomTime()) // trx_time
	builder.WriteString("',")
	builder.WriteString(strconv.FormatInt(int64(randomInt()), 10)) //ctd_stmt_stat_cycle
	builder.WriteString(",")
	builder.WriteString(strconv.FormatInt(int64(randomInt()), 10)) //cash_adv_cr_trx_count
	builder.WriteString(",'")
	builder.WriteString(randomString(32)) // upd_tlr_no
	builder.WriteString("',")
	builder.WriteString(strconv.FormatInt(int64(randomInt()), 10)) //ver_no
	builder.WriteString(",'")
	builder.WriteString(randomString(32)) // create_tlr_no
	builder.WriteString("',")
	builder.WriteString(randomDecimal(13, 2)) // acc_cash_bal

	return builder.String()*/
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
	// var builder strings.Builder
	// builder.WriteString("'")
	// builder.WriteString(randomString(16)) // crcd_acct_no
	// builder.WriteString("','")
	// builder.WriteString(randomDate()) // grace_date
	// builder.WriteString("','")
	// builder.WriteString(randomString(3)) // acct_seq_no
	// builder.WriteString("',")
	// builder.WriteString(strconv.FormatInt(randomBigInt(), 10)) //stmt_head_id
	// builder.WriteString(",")

	// crcd_acct_no , stmt_date , acct_seq_no , stmt_head_id  these are primary key
	values := valuesPool.Get().([]interface{})
	defer valuesPool.Put(values[:0]) // 使用后重置并放回池中

	values = append(values, randomString(16)) // crcd_acct_no
	values = append(values, randomDate())     // stmt_date
	values = append(values, randomString(3))  // acct_seq_no
	values = append(values, randomBigInt())   // stmt_head_id
	return values
	//return builder.String()
}

func generateValuesForLogTable() []interface{} {
	// `crcd_acct_no`,`create_date`,`id` is primary key
	values := make([]interface{}, 0, 45)
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
	values = append(values, randomBigInt())       // id
	values = append(values, randomDate())         // create_date
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
	values = append(values, randomString(16))     // crcd_acct_no
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
	// seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))
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
	// rand.Seed(time.Now().UnixNano())
	min := int32(0)
	max := int32(1<<31 - 1)
	return rand.Int31n(max-min) + min
}

func randomSmallInt() int16 {
	// rand.Seed(time.Now().UnixNano())
	raw := rand.Int31n(65536)
	smallintValue := int16(raw - 32768)
	return int16(smallintValue)
}

func randomDecimal(precision, scale int) string {
	// rand.Seed(time.Now().UnixNano())

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

	// 根据精度格式化时间
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
		format += ".000000" // 默认微秒精度
	}

	return time.Unix(sec, 0).Format(format)
}

func randomTime() string {
	// rand.Seed(time.Now().UnixNano())
	start := time.Now().AddDate(-40, 0, 0)
	end := time.Now()
	delta := end.Sub(start)
	randomDuration := time.Duration(rand.Int63n(int64(delta)))
	return start.Add(randomDuration).Format("15:04:05")
}
