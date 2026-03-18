// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package contentmetadatawide

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	plog "github.com/pingcap/log"
	"go.uber.org/zap"
	"workload/schema"
)

const (
	defaultContentMetadataWideRowSize        = 10 * 1024
	defaultContentMetadataWideUpdateKeySpace = 1_000_000
	recordIDBase                             = int64(-9_000_000_000_000_000_000)

	updateImagesWithExistenceWeight = 0.49
	updateStatusOnlyWeight          = 0.29
	updateImagesOnlyWeight          = 0.18
	updateWideMetadataWeight        = 0.03
	// remaining is metadata+varys update
)

const createContentMetadataWideTableFormat = `
CREATE TABLE IF NOT EXISTS %s (
  ` + "`entity_id`" + ` bigint NOT NULL,
  ` + "`created_at`" + ` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ` + "`updated_at`" + ` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  ` + "`active_flag`" + ` tinyint(1) NOT NULL,
  ` + "`external_group_ids`" + ` varbinary(40131) DEFAULT NULL,
  ` + "`external_code`" + ` varbinary(1556) DEFAULT NULL,
  ` + "`external_type`" + ` varbinary(24) DEFAULT NULL,
  ` + "`aggregated_link_data`" + ` mediumblob DEFAULT NULL,
  ` + "`media_bundle_data`" + ` varbinary(44365) DEFAULT NULL,
  ` + "`alternate_link_data`" + ` mediumblob DEFAULT NULL,
  ` + "`entry_link`" + ` varbinary(8009) DEFAULT NULL,
  ` + "`entry_link_flag`" + ` varbinary(5) DEFAULT NULL,
  ` + "`anchor_data`" + ` mediumblob DEFAULT NULL,
  ` + "`icon_store_data_a`" + ` varbinary(455) DEFAULT NULL,
  ` + "`icon_store_ref_a`" + ` varbinary(327) DEFAULT NULL,
  ` + "`icon_link_a`" + ` mediumblob DEFAULT NULL,
  ` + "`content_block_a`" + ` mediumblob DEFAULT NULL,
  ` + "`attribution_data`" + ` varbinary(5311) DEFAULT NULL,
  ` + "`content_block_b`" + ` varbinary(6335) DEFAULT NULL,
  ` + "`aux_text_data_a`" + ` varbinary(3617) DEFAULT NULL,
  ` + "`related_item_blob_a`" + ` mediumblob DEFAULT NULL,
  ` + "`canonical_link`" + ` varbinary(8005) DEFAULT NULL,
  ` + "`category_vector_data`" + ` varbinary(578) DEFAULT NULL,
  ` + "`client_route_data`" + ` varbinary(28387) DEFAULT NULL,
  ` + "`continuous_error_count`" + ` varbinary(4) DEFAULT NULL,
  ` + "`fetch_latency_ms`" + ` varbinary(5) DEFAULT NULL,
  ` + "`summary_language_vector`" + ` varbinary(760) DEFAULT NULL,
  ` + "`summary_text`" + ` mediumblob DEFAULT NULL,
  ` + "`aux_blob_a`" + ` varbinary(27360) DEFAULT NULL,
  ` + "`script_redirect_flag`" + ` varbinary(5) DEFAULT NULL,
  ` + "`signal_summary`" + ` varbinary(187) DEFAULT NULL,
  ` + "`icon_store_data_b`" + ` varbinary(533) DEFAULT NULL,
  ` + "`icon_store_ref_b`" + ` varbinary(405) DEFAULT NULL,
  ` + "`icon_link_b`" + ` mediumblob DEFAULT NULL,
  ` + "`footer_digest`" + ` varbinary(4) DEFAULT NULL,
  ` + "`text_image_urls`" + ` mediumblob DEFAULT NULL,
  ` + "`media_index_blob`" + ` varbinary(2250) DEFAULT NULL,
  ` + "`media_map_blob`" + ` varbinary(2808) DEFAULT NULL,
  ` + "`headline_text`" + ` mediumblob DEFAULT NULL,
  ` + "`headline_language_vectors`" + ` mediumblob DEFAULT NULL,
  ` + "`hidden_flag`" + ` varbinary(5) DEFAULT NULL,
  ` + "`html_image_data`" + ` mediumblob DEFAULT NULL,
  ` + "`html_theme_data`" + ` varbinary(13260) DEFAULT NULL,
  ` + "`http_status`" + ` varbinary(4) DEFAULT NULL,
  ` + "`image_probe_result`" + ` mediumblob DEFAULT NULL,
  ` + "`browser_runtime_data`" + ` mediumblob DEFAULT NULL,
  ` + "`initial_text_a`" + ` mediumblob DEFAULT NULL,
  ` + "`initial_blob_a`" + ` mediumblob DEFAULT NULL,
  ` + "`initial_fetch_time`" + ` varbinary(28) DEFAULT NULL,
  ` + "`initial_text_b`" + ` mediumblob DEFAULT NULL,
  ` + "`instant_blob_a`" + ` varbinary(19370) DEFAULT NULL,
  ` + "`instant_content_data`" + ` mediumblob DEFAULT NULL,
  ` + "`quality_label_data`" + ` varbinary(189) DEFAULT NULL,
  ` + "`initial_fetch_flag`" + ` varbinary(5) DEFAULT NULL,
  ` + "`script_confidence_score`" + ` varbinary(21) DEFAULT NULL,
  ` + "`script_render_helpful`" + ` varbinary(5) DEFAULT NULL,
  ` + "`topic_bucket_l1`" + ` varbinary(330) DEFAULT NULL,
  ` + "`landing_link`" + ` varbinary(39998) DEFAULT NULL,
  ` + "`last_delete_time`" + ` varbinary(28) DEFAULT NULL,
  ` + "`last_disable_time`" + ` varbinary(28) DEFAULT NULL,
  ` + "`last_error_time`" + ` varbinary(28) DEFAULT NULL,
  ` + "`source_modified_time`" + ` varbinary(28) DEFAULT NULL,
  ` + "`layout_block_data`" + ` varbinary(2789) DEFAULT NULL,
  ` + "`link_state`" + ` varbinary(2) DEFAULT NULL,
  ` + "`locale_code`" + ` varbinary(11) DEFAULT NULL,
  ` + "`body_data`" + ` mediumblob DEFAULT NULL,
  ` + "`metadata_quality_score`" + ` varbinary(3) DEFAULT NULL,
  ` + "`metadata_revision`" + ` varbinary(20) DEFAULT NULL,
  ` + "`client_profile_data`" + ` varbinary(2057) DEFAULT NULL,
  ` + "`client_capability_data`" + ` varbinary(27) DEFAULT NULL,
  ` + "`aux_blob_b`" + ` mediumblob DEFAULT NULL,
  ` + "`ad_count_data`" + ` varbinary(58) DEFAULT NULL,
  ` + "`template_content_ratio`" + ` varbinary(22) DEFAULT NULL,
  ` + "`page_image_data`" + ` mediumblob DEFAULT NULL,
  ` + "`entity_reference_blob`" + ` varbinary(49369) DEFAULT NULL,
  ` + "`media_asset_blob`" + ` varbinary(4545) DEFAULT NULL,
  ` + "`geo_reference_blob`" + ` mediumblob DEFAULT NULL,
  ` + "`related_link_blob`" + ` mediumblob DEFAULT NULL,
  ` + "`related_item_score`" + ` varbinary(22) DEFAULT NULL,
  ` + "`related_item_blob_b`" + ` mediumblob DEFAULT NULL,
  ` + "`content_history_blob`" + ` mediumblob DEFAULT NULL,
  ` + "`publish_time`" + ` varbinary(21) DEFAULT NULL,
  ` + "`raw_image_data`" + ` mediumblob DEFAULT NULL,
  ` + "`aux_blob_c`" + ` mediumblob DEFAULT NULL,
  ` + "`redirect_link_data`" + ` mediumblob DEFAULT NULL,
  ` + "`feed_data`" + ` mediumblob DEFAULT NULL,
  ` + "`source_label`" + ` mediumblob DEFAULT NULL,
  ` + "`soft_error_flag`" + ` varbinary(28) DEFAULT NULL,
  ` + "`media_collection_blob`" + ` varbinary(59942) DEFAULT NULL,
  ` + "`spam_risk_score`" + ` varbinary(22) DEFAULT NULL,
  ` + "`language_region_code`" + ` varbinary(4) DEFAULT NULL,
  ` + "`fetch_status_code`" + ` varbinary(4) DEFAULT NULL,
  ` + "`inventory_signal`" + ` varbinary(1) DEFAULT NULL,
  ` + "`title_text`" + ` mediumblob DEFAULT NULL,
  ` + "`tracking_data`" + ` varbinary(414) DEFAULT NULL,
  ` + "`trust_type`" + ` varbinary(21) DEFAULT NULL,
  ` + "`guide_blob`" + ` varbinary(20040) DEFAULT NULL,
  ` + "`aux_blob_d`" + ` varbinary(41326) DEFAULT NULL,
  ` + "`source_url`" + ` varbinary(8192) DEFAULT NULL,
  ` + "`source_token_data`" + ` varbinary(28769) DEFAULT NULL,
  ` + "`variant_image_data`" + ` mediumblob DEFAULT NULL,
  ` + "`media_blob_a`" + ` mediumblob DEFAULT NULL,
  ` + "`media_blob_b`" + ` varbinary(100) DEFAULT NULL,
  ` + "`source_url_hash`" + ` binary(16) DEFAULT NULL,
  PRIMARY KEY (` + "`entity_id`" + `) /*T![clustered_index] CLUSTERED */,
  UNIQUE KEY ` + "`idx_record_hash_active`" + ` (` + "`source_url_hash`" + `,` + "`active_flag`" + `)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
`

type payloadSizes struct {
	pageImages              int
	imageProbeResult        int
	rawImageData            int
	variantImageData        int
	siteIconURL             int
	summaryText             int
	anchorData              int
	titleText               int
	productData             int
	htmlImageData           int
	sourceTokenData         int
	redirectLinkData        int
	bodyData                int
	mobileIconURL           int
	summaryLanguageVector   int
	siteLabel               int
	headlineText            int
	headlineLanguageVectors int
	canonicalLink           int
	landingLink             int
	mobilePageURL           int
}

type ContentMetadataWideWorkload struct {
	tableStartIndex int

	insertedSeq []atomic.Uint64

	perTableUpdateKeySpace uint64
	sizes                  payloadSizes

	seed     atomic.Int64
	randPool sync.Pool
}

func NewContentMetadataWideWorkload(rowSize int, tableCount int, tableStartIndex int, totalRowCount uint64) schema.Workload {
	if rowSize <= 0 {
		rowSize = defaultContentMetadataWideRowSize
	}
	if tableCount <= 0 {
		tableCount = 1
	}

	perTableUpdateKeySpace := uint64(defaultContentMetadataWideUpdateKeySpace)
	if totalRowCount > 0 {
		perTableUpdateKeySpace = maxUint64(1, totalRowCount/uint64(tableCount))
	}

	w := &ContentMetadataWideWorkload{
		tableStartIndex:        tableStartIndex,
		insertedSeq:            make([]atomic.Uint64, tableCount),
		perTableUpdateKeySpace: perTableUpdateKeySpace,
		sizes:                  derivePayloadSizes(rowSize),
	}
	w.seed.Store(time.Now().UnixNano())
	w.randPool.New = func() any {
		return rand.New(rand.NewSource(w.seed.Add(1)))
	}

	plog.Info("content_metadata_wide workload initialized",
		zap.Int("rowSize", rowSize),
		zap.Int("tableSlots", len(w.insertedSeq)),
		zap.Int("tableStartIndex", w.tableStartIndex),
		zap.Uint64("perTableUpdateKeySpace", w.perTableUpdateKeySpace),
		zap.Int("pageImagesSize", w.sizes.pageImages),
		zap.Int("rawImageDataSize", w.sizes.rawImageData),
		zap.Int("imageProbeResultSize", w.sizes.imageProbeResult),
		zap.Int("bodyDataSize", w.sizes.bodyData))
	return w
}

func derivePayloadSizes(rowSize int) payloadSizes {
	return payloadSizes{
		pageImages:              clampSize(maxInt(1536, rowSize/4), 1024, 64*1024),
		imageProbeResult:        clampSize(maxInt(1024, rowSize/8), 512, 32*1024),
		rawImageData:            clampSize(maxInt(2048, rowSize/4), 1024, 64*1024),
		variantImageData:        clampSize(maxInt(1024, rowSize/8), 256, 32*1024),
		siteIconURL:             clampSize(maxInt(96, rowSize/48), 32, 768),
		summaryText:             clampSize(maxInt(256, rowSize/20), 64, 4096),
		anchorData:              clampSize(maxInt(128, rowSize/24), 64, 2048),
		titleText:               clampSize(maxInt(96, rowSize/40), 32, 1024),
		productData:             clampSize(maxInt(128, rowSize/24), 64, 4096),
		htmlImageData:           clampSize(maxInt(128, rowSize/24), 64, 4096),
		sourceTokenData:         clampSize(maxInt(192, rowSize/16), 64, 4096),
		redirectLinkData:        clampSize(maxInt(384, rowSize/12), 128, 8192),
		bodyData:                clampSize(maxInt(2048, rowSize/3), 1024, 64*1024),
		mobileIconURL:           clampSize(maxInt(96, rowSize/48), 32, 768),
		summaryLanguageVector:   clampSize(maxInt(64, rowSize/32), 32, 760),
		siteLabel:               clampSize(maxInt(48, rowSize/64), 16, 256),
		headlineText:            clampSize(maxInt(192, rowSize/24), 64, 4096),
		headlineLanguageVectors: clampSize(maxInt(1536, rowSize/4), 512, 32*1024),
		canonicalLink:           clampSize(maxInt(96, rowSize/40), 32, 1024),
		landingLink:             clampSize(maxInt(96, rowSize/40), 32, 1024),
		mobilePageURL:           clampSize(maxInt(32, rowSize/80), 16, 256),
	}
}

func (w *ContentMetadataWideWorkload) BuildCreateTableStatement(n int) string {
	return fmt.Sprintf(createContentMetadataWideTableFormat, tableName(n))
}

func (w *ContentMetadataWideWorkload) BuildInsertSql(tableN int, batchSize int) string {
	return fmt.Sprintf("INSERT INTO %s (`entity_id`, `active_flag`) VALUES (%d, 1)",
		tableName(tableN), time.Now().UnixNano())
}

func (w *ContentMetadataWideWorkload) BuildInsertSqlWithValues(tableIndex int, batchSize int) (string, []interface{}) {
	if batchSize <= 0 {
		batchSize = 1
	}

	r := w.getRand()
	defer w.putRand(r)

	tableName := tableName(tableIndex)
	slot := w.slot(tableIndex)

	const columnCount = 24
	values := make([]interface{}, 0, batchSize*columnCount)
	place_dataholders := make([]string, 0, batchSize)

	sql := fmt.Sprintf("INSERT INTO %s (`entity_id`, `active_flag`, `metadata_revision`, `page_image_data`, `image_probe_result`, `raw_image_data`, `variant_image_data`, `fetch_status_code`, `source_modified_time`, `source_url`, `canonical_link`, `landing_link`, `source_label`, `title_text`, `summary_text`, `body_data`, `source_token_data`, `redirect_link_data`, `icon_link_a`, `summary_language_vector`, `headline_text`, `headline_language_vectors`, `locale_code`, `source_url_hash`) VALUES ",
		tableName)

	for i := 0; i < batchSize; i++ {
		seq := w.insertedSeq[slot].Add(1)
		now := time.Now().Add(time.Duration(i) * time.Millisecond)
		recordID := w.recordID(tableIndex, seq)
		place_dataholders = append(place_dataholders, "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
		values = append(values,
			recordID,
			1,
			metadataRevisionValue(seq),
			buildImagePayload(w.sizes.pageImages, tableIndex, seq, "page"),
			buildImageProbePayload(w.sizes.imageProbeResult, seq, now),
			buildImagePayload(w.sizes.rawImageData, tableIndex, seq, "raw"),
			w.maybeVarysImages(r, tableIndex, seq),
			fetchStatusCodeValue(seq),
			quotedDateTimeValue(now),
			sourceURLValue("node", tableIndex, seq, 320),
			sourceURLValue("canonical", tableIndex, seq, w.sizes.canonicalLink),
			sourceURLValue("landing", tableIndex, seq, w.sizes.landingLink),
			buildQuotedText(w.sizes.siteLabel, fmt.Sprintf("source-%d.example", tableIndex), seq),
			buildQuotedText(w.sizes.titleText, "Synthetic content title", seq),
			buildQuotedText(w.sizes.summaryText, "Synthetic content summary payload", seq),
			buildBodyPayload(w.sizes.bodyData, tableIndex, seq),
			buildSourceTokenPayload(w.sizes.sourceTokenData, tableIndex, seq),
			buildRedirectLinkPayload(w.sizes.redirectLinkData, tableIndex, seq),
			sourceURLValue("mobile-icon", tableIndex, seq, w.sizes.mobileIconURL),
			buildLanguageVectorDataPayload(w.sizes.summaryLanguageVector, seq),
			buildHeadlinePayload(w.sizes.headlineText, seq),
			buildHeadlineLanguageVectorPayload(w.sizes.headlineLanguageVectors, seq),
			[]byte(`"en"`),
			sourceURLHashValue(tableIndex, seq),
		)
	}

	return sql + strings.Join(place_dataholders, ","), values
}

func (w *ContentMetadataWideWorkload) BuildUpdateSql(opt schema.UpdateOption) string {
	return fmt.Sprintf("UPDATE %s SET `fetch_status_code` = '200' WHERE (`entity_id` = 0)", tableName(opt.TableIndex))
}

func (w *ContentMetadataWideWorkload) BuildUpdateSqlWithValues(opt schema.UpdateOption) (string, []interface{}) {
	r := w.getRand()
	defer w.putRand(r)

	slot := w.slot(opt.TableIndex)
	upper := minUint64(w.perTableUpdateKeySpace, w.insertedSeq[slot].Load())
	seq := randSeq(r, upper)
	now := time.Now()
	tableName := tableName(opt.TableIndex)
	recordID := w.recordID(opt.TableIndex, seq)

	p := r.Float64()
	switch {
	case p < updateImagesWithExistenceWeight:
		sql := fmt.Sprintf("UPDATE %s SET `page_image_data` = ?, `metadata_revision` = ?, `image_probe_result` = ?, `raw_image_data` = ?, `variant_image_data` = ? WHERE (`entity_id` = ?)", tableName)
		return sql, []interface{}{
			buildImagePayload(w.sizes.pageImages, opt.TableIndex, seq, "page"),
			metadataRevisionValue(uint64(now.UnixNano())),
			buildImageProbePayload(w.sizes.imageProbeResult, seq, now),
			buildImagePayload(w.sizes.rawImageData, opt.TableIndex, seq, "raw"),
			w.maybeVarysImages(r, opt.TableIndex, seq+11),
			recordID,
		}
	case p < updateImagesWithExistenceWeight+updateStatusOnlyWeight:
		sql := fmt.Sprintf("UPDATE %s SET `fetch_status_code` = ?, `source_modified_time` = ? WHERE (`entity_id` = ?)", tableName)
		return sql, []interface{}{
			fetchStatusCodeValue(uint64(now.UnixNano())),
			quotedDateTimeValue(now),
			recordID,
		}
	case p < updateImagesWithExistenceWeight+updateStatusOnlyWeight+updateImagesOnlyWeight:
		sql := fmt.Sprintf("UPDATE %s SET `page_image_data` = ?, `metadata_revision` = ?, `raw_image_data` = ?, `variant_image_data` = ? WHERE (`entity_id` = ?)", tableName)
		return sql, []interface{}{
			buildImagePayload(w.sizes.pageImages, opt.TableIndex, seq+1, "page"),
			metadataRevisionValue(uint64(now.UnixNano())),
			buildImagePayload(w.sizes.rawImageData, opt.TableIndex, seq+1, "raw"),
			w.maybeVarysImages(r, opt.TableIndex, seq+17),
			recordID,
		}
	case p < updateImagesWithExistenceWeight+updateStatusOnlyWeight+updateImagesOnlyWeight+updateWideMetadataWeight:
		sql := fmt.Sprintf("UPDATE %s SET `template_content_ratio` = ?, `related_item_score` = ?, `icon_link_b` = ?, `summary_text` = ?, `link_state` = ?, `metadata_quality_score` = ?, `anchor_data` = ?, `locale_code` = ?, `title_text` = ?, `raw_image_data` = ?, `related_item_blob_b` = ?, `html_image_data` = ?, `source_token_data` = ?, `redirect_link_data` = ?, `metadata_revision` = ?, `script_confidence_score` = ?, `fetch_latency_ms` = ?, `source_modified_time` = ?, `body_data` = ?, `http_status` = ?, `entry_link` = ?, `icon_link_a` = ?, `summary_language_vector` = ?, `canonical_link` = ?, `landing_link` = ?, `alternate_link_data` = ?, `source_label` = ?, `script_redirect_flag` = ?, `inventory_signal` = ?, `headline_text` = ?, `soft_error_flag` = ?, `headline_language_vectors` = ? WHERE (`entity_id` = ?)", tableName)
		return sql, []interface{}{
			scoreValue(r, 4),
			scoreValue(r, 5),
			sourceURLValue("site-icon", opt.TableIndex, seq, w.sizes.siteIconURL),
			buildQuotedText(w.sizes.summaryText, "Synthetic content summary", seq),
			linkStateValue(seq),
			qualityScoreValue(r),
			buildAnchorDataPayload(w.sizes.anchorData, opt.TableIndex, seq),
			localeCodeValue(seq),
			buildQuotedText(w.sizes.titleText, "Synthetic content title", seq),
			buildImagePayload(w.sizes.rawImageData, opt.TableIndex, seq, "raw"),
			buildProductDataPayload(w.sizes.productData, opt.TableIndex, seq),
			buildHTMLImageDataPayload(w.sizes.htmlImageData, opt.TableIndex, seq),
			buildSourceTokenPayload(w.sizes.sourceTokenData, opt.TableIndex, seq),
			buildRedirectLinkPayload(w.sizes.redirectLinkData, opt.TableIndex, seq),
			metadataRevisionValue(uint64(now.UnixNano())),
			scoreValue(r, 6),
			fetchLatencyValue(r),
			quotedDateTimeValue(now),
			buildBodyPayload(w.sizes.bodyData, opt.TableIndex, seq),
			fetchStatusCodeValue(seq + 7),
			sourceURLValue("mobile-page", opt.TableIndex, seq, w.sizes.mobilePageURL),
			sourceURLValue("mobile-icon", opt.TableIndex, seq, w.sizes.mobileIconURL),
			buildLanguageVectorDataPayload(w.sizes.summaryLanguageVector, seq),
			sourceURLValue("canonical", opt.TableIndex, seq, w.sizes.canonicalLink),
			sourceURLValue("landing", opt.TableIndex, seq, w.sizes.landingLink),
			buildAlternateLinkPayload(160, opt.TableIndex, seq),
			buildQuotedText(w.sizes.siteLabel, fmt.Sprintf("source-%d.example", opt.TableIndex), seq),
			boolValue(seq%2 == 0),
			inventorySignalValue(seq),
			buildHeadlinePayload(w.sizes.headlineText, seq),
			softErrorFlagValue(seq),
			buildHeadlineLanguageVectorPayload(w.sizes.headlineLanguageVectors, seq),
			recordID,
		}
	default:
		sql := fmt.Sprintf("UPDATE %s SET `metadata_revision` = ?, `variant_image_data` = ? WHERE (`entity_id` = ?)", tableName)
		return sql, []interface{}{
			metadataRevisionValue(uint64(now.UnixNano())),
			w.maybeVarysImages(r, opt.TableIndex, seq+31),
			recordID,
		}
	}
}

func (w *ContentMetadataWideWorkload) BuildDeleteSql(opt schema.DeleteOption) string {
	slot := w.slot(opt.TableIndex)
	upper := minUint64(w.perTableUpdateKeySpace, w.insertedSeq[slot].Load())
	if upper == 0 {
		return ""
	}

	batch := maxInt(1, opt.Batch)
	parts := make([]string, 0, batch)
	r := w.getRand()
	defer w.putRand(r)
	for i := 0; i < batch; i++ {
		seq := randSeq(r, upper)
		recordID := w.recordID(opt.TableIndex, seq)
		parts = append(parts, fmt.Sprintf("DELETE FROM %s WHERE (`entity_id` = %d)", tableName(opt.TableIndex), recordID))
	}
	return strings.Join(parts, ";")
}

func (w *ContentMetadataWideWorkload) maybeVarysImages(r *rand.Rand, tableIndex int, seq uint64) interface{} {
	if r.Float64() < 0.58 {
		return nil
	}
	return buildImagePayload(w.sizes.variantImageData, tableIndex, seq, "variant")
}

func (w *ContentMetadataWideWorkload) slot(tableIndex int) int {
	if len(w.insertedSeq) == 0 {
		return 0
	}
	slot := tableIndex - w.tableStartIndex
	if slot < 0 {
		slot = -slot
	}
	return slot % len(w.insertedSeq)
}

func (w *ContentMetadataWideWorkload) recordID(tableIndex int, seq uint64) int64 {
	return recordIDBase + int64(tableIndex)*1_000_000_000 + int64(seq)
}

func (w *ContentMetadataWideWorkload) getRand() *rand.Rand {
	return w.randPool.Get().(*rand.Rand)
}

func (w *ContentMetadataWideWorkload) putRand(r *rand.Rand) {
	w.randPool.Put(r)
}

func tableName(index int) string {
	if index == 0 {
		return "`content_metadata_wide`"
	}
	return fmt.Sprintf("`content_metadata_wide_%d`", index)
}

func sourceURLHashValue(tableIndex int, seq uint64) []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[:8], uint64(tableIndex))
	binary.BigEndian.PutUint64(buf[8:], seq)
	return buf
}

func metadataRevisionValue(seq uint64) []byte {
	return []byte(fmt.Sprintf("%020d", seq%10_000_000_000_000_000_000))
}

func fetchStatusCodeValue(seq uint64) []byte {
	codes := [...]string{"200", "204", "301", "302", "404", "410", "500"}
	return []byte(codes[seq%uint64(len(codes))])
}

func quotedDateTimeValue(t time.Time) []byte {
	return []byte(`"` + t.UTC().Format("2006-01-02 15:04:05.000000") + `"`)
}

func linkStateValue(seq uint64) []byte {
	return []byte(fmt.Sprintf("%d", seq%4))
}

func qualityScoreValue(r *rand.Rand) []byte {
	return []byte(fmt.Sprintf("%02d", 80+r.Intn(21)))
}

func scoreValue(r *rand.Rand, precision int) []byte {
	return []byte(fmt.Sprintf("%.*f", precision, r.Float64()))
}

func fetchLatencyValue(r *rand.Rand) []byte {
	return []byte(fmt.Sprintf("%d", 40+r.Intn(4000)))
}

func localeCodeValue(seq uint64) []byte {
	locales := [...]string{`"en"`, `"en_US"`, `"en_GB"`, `"es"`, `"fr"`}
	return []byte(locales[seq%uint64(len(locales))])
}

func boolValue(v bool) []byte {
	if v {
		return []byte("true")
	}
	return []byte("false")
}

func inventorySignalValue(seq uint64) []byte {
	return []byte(fmt.Sprintf("%d", seq%2))
}

func softErrorFlagValue(seq uint64) []byte {
	value := int64(seq%3) - 1
	return []byte(fmt.Sprintf(`{"v":%d,"w":0.%d}`, value, seq%10))
}

func sourceURLValue(kind string, tableIndex int, seq uint64, target int) []byte {
	base := fmt.Sprintf("https://%s-%d.content.example.invalid/%d/%d/index.html", kind, tableIndex, seq%1024, seq)
	return buildQuotedText(target, base, seq)
}

func buildQuotedText(target int, prefix string, seq uint64) []byte {
	text := fmt.Sprintf("%s %d ", prefix, seq)
	minSize := len(text) + 2
	if target < minSize {
		target = minSize
	}

	var b strings.Builder
	b.Grow(target)
	b.WriteByte('"')
	for b.Len()+len(text)+1 <= target {
		b.WriteString(text)
	}
	if b.Len() == 1 {
		b.WriteString(text)
	}
	b.WriteByte('"')
	return []byte(b.String())
}

func buildImagePayload(target int, tableIndex int, seq uint64, kind string) []byte {
	return buildJSONArray(target, func(i int) string {
		return fmt.Sprintf(`{"kind":"%s","source_url":"https://img-%d.content.example.invalid/%s/%d/%d.jpg","width":1200,"height":630,"digest":"%016x","status":200}`,
			kind, tableIndex, kind, seq, i, seq+uint64(i))
	})
}

func buildImageProbePayload(target int, seq uint64, now time.Time) []byte {
	return buildJSONObject(target, func(i int) string {
		switch i {
		case 0:
			return fmt.Sprintf(`"last_checked":"%s"`, now.UTC().Format(time.RFC3339Nano))
		case 1:
			return fmt.Sprintf(`"image_count":%d`, 1+seq%8)
		case 2:
			return fmt.Sprintf(`"reachable":%t`, seq%2 == 0)
		default:
			return fmt.Sprintf(`"probe_%d":{"ok":true,"digest":"%016x"}`, i, seq+uint64(i))
		}
	})
}

func buildAnchorDataPayload(target int, tableIndex int, seq uint64) []byte {
	return buildJSONArray(target, func(i int) string {
		return fmt.Sprintf(`{"text":"anchor-%d-%d","href":"https://link-%d.content.example.invalid/%d/%d","rel":"nofollow"}`,
			seq, i, tableIndex, seq, i)
	})
}

func buildProductDataPayload(target int, tableIndex int, seq uint64) []byte {
	return buildJSONArray(target, func(i int) string {
		return fmt.Sprintf(`{"sku":"sku-%d-%d","merchant":"merchant-%d","price":"%d.%02d"}`,
			seq, i, tableIndex, 10+int(seq%90), i%100)
	})
}

func buildHTMLImageDataPayload(target int, tableIndex int, seq uint64) []byte {
	return buildJSONArray(target, func(i int) string {
		return fmt.Sprintf(`{"src":"https://html-%d.content.example.invalid/%d/%d.png","alt":"html image %d","lazy":true}`,
			tableIndex, seq, i, i)
	})
}

func buildSourceTokenPayload(target int, tableIndex int, seq uint64) []byte {
	return buildJSONArray(target, func(i int) string {
		return fmt.Sprintf(`"content-%d-token-%d-%d"`, tableIndex, seq, i)
	})
}

func buildRedirectLinkPayload(target int, tableIndex int, seq uint64) []byte {
	return buildJSONArray(target, func(i int) string {
		return fmt.Sprintf(`"https://redirect-%d.content.example.invalid/%d/%d"`, tableIndex, seq, i)
	})
}

func buildAlternateLinkPayload(target int, tableIndex int, seq uint64) []byte {
	return buildJSONObject(target, func(i int) string {
		return fmt.Sprintf(`"alt_%d":"https://alt-%d.content.example.invalid/%d/%d"`, i, tableIndex, seq, i)
	})
}

func buildBodyPayload(target int, tableIndex int, seq uint64) []byte {
	return buildJSONObject(target, func(i int) string {
		switch i {
		case 0:
			return fmt.Sprintf(`"body":"segment-%d-%d segment-%d-%d segment-%d-%d"`, tableIndex, seq, tableIndex, seq+1, tableIndex, seq+2)
		case 1:
			return fmt.Sprintf(`"image_count":%d`, 1+seq%16)
		case 2:
			languages := [...]string{"en", "en_US", "es"}
			return fmt.Sprintf(`"language":"%s"`, languages[int(seq%uint64(len(languages)))])
		default:
			return fmt.Sprintf(`"block_%d":"content-%d-%d"`, i, seq, i)
		}
	})
}

func buildLanguageVectorDataPayload(target int, seq uint64) []byte {
	return buildJSONObject(target, func(i int) string {
		langs := [...]string{"EN", "ES", "FR", "DE"}
		lang := langs[i%len(langs)]
		return fmt.Sprintf(`"%s":0.%03d`, lang, (int(seq)+i*17)%1000)
	})
}

func buildHeadlinePayload(target int, seq uint64) []byte {
	return buildJSONArray(target, func(i int) string {
		return fmt.Sprintf(`"headline-%d-%d synthetic metadata workload"`, seq, i)
	})
}

func buildHeadlineLanguageVectorPayload(target int, seq uint64) []byte {
	return buildJSONArray(target, func(i int) string {
		return fmt.Sprintf(`{"lang":"%s","score":0.%03d}`,
			[]string{"EN", "ES", "FR", "DE"}[i%4], (int(seq)+i*23)%1000)
	})
}

func buildJSONArray(target int, item func(i int) string) []byte {
	if target < 4 {
		target = 4
	}

	var b strings.Builder
	b.Grow(target)
	b.WriteByte('[')
	for i := 0; ; i++ {
		part := item(i)
		if i > 0 {
			part = "," + part
		}
		if b.Len()+len(part)+1 > target {
			if i == 0 {
				b.WriteString(part)
			}
			break
		}
		b.WriteString(part)
	}
	b.WriteByte(']')
	return []byte(b.String())
}

func buildJSONObject(target int, field func(i int) string) []byte {
	if target < 4 {
		target = 4
	}

	var b strings.Builder
	b.Grow(target)
	b.WriteByte('{')
	for i := 0; ; i++ {
		part := field(i)
		if i > 0 {
			part = "," + part
		}
		if b.Len()+len(part)+1 > target {
			if i == 0 {
				b.WriteString(part)
			}
			break
		}
		b.WriteString(part)
	}
	b.WriteByte('}')
	return []byte(b.String())
}

func randSeq(r *rand.Rand, upper uint64) uint64 {
	if upper <= 1 {
		return 1
	}
	if upper <= uint64(math.MaxInt64) {
		return uint64(r.Int63n(int64(upper))) + 1
	}
	return (r.Uint64() % upper) + 1
}

func clampSize(v int, minV int, maxV int) int {
	return minInt(maxInt(v, minV), maxV)
}

func maxInt(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func minUint64(a uint64, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxUint64(a uint64, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
