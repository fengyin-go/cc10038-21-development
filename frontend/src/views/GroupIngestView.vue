<template>
  <section class="page" data-module="group_ingest">
    <header class="page-head">
      <div>
        <h2>成组多源采集</h2>
        <p class="page-desc">
          多选数据源一次提交，后端按实际来源逐条分批接收；凭证未就绪的来源先占用接入资格并暂存原文，
          凭证通过后用原组编号补送，齐备后正式记录才可见。
        </p>
      </div>
    </header>

    <form class="group-form" @submit.prevent="submit">
      <div class="form-row">
        <label class="filter-item">
          <span>成组编号</span>
          <input v-model="groupNo" data-testid="group-no" placeholder="如 GRP-PLANT-20261121" required />
        </label>
        <label class="filter-item">
          <span>采集时间窗口</span>
          <input v-model="timeWindow" data-testid="time-window" placeholder="如 2026-11-21T08:00" required />
        </label>
      </div>

      <div class="source-picker">
        <div class="picker-head">
          <span>多选来源（每个来源独立分批，原文每行一条）</span>
          <button class="btn ghost" type="button" data-testid="add-source" @click="addSource">
            + 添加来源
          </button>
        </div>
        <div v-for="(item, idx) in sourceInputs" :key="idx" class="source-row">
          <label class="filter-item">
            <span>来源标识</span>
            <input v-model="item.source_id" :data-testid="`source-id-${idx}`" placeholder="数据源来源标识" required />
          </label>
          <label class="filter-item logs-field">
            <span>原始日志（每行一条）</span>
            <textarea
              v-model="item.logsText"
              :data-testid="`source-logs-${idx}`"
              rows="3"
              placeholder="2026-11-21T08:00|INFO|line one"
            ></textarea>
          </label>
          <button class="link danger" type="button" @click="removeSource(idx)">移除</button>
        </div>
      </div>

      <div class="page-actions">
        <button class="btn primary" type="submit" data-testid="submit-group" :disabled="submitting">
          {{ submitting ? '提交中…' : '成组提交' }}
        </button>
        <button class="btn" type="button" data-testid="supplement-group" :disabled="submitting || !groupNo" @click="supplement">
          用原组编号补送
        </button>
      </div>
      <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>
    </form>

    <div v-if="receipt" class="receipt-panel" data-testid="receipt-panel">
      <h3>成组采集回执 · {{ receipt.group_no }}</h3>
      <p class="status-legend">
        <span class="legend-item">组状态：{{ receipt.group_status }}</span>
        <span class="legend-item">正式记录可见：{{ receipt.formal_visible ? '是' : '否' }}</span>
        <span class="legend-item">重复提交：{{ receipt.is_resubmit ? '是（幂等）' : '否' }}</span>
        <span class="legend-item">尚缺来源：{{ receipt.missing_sources.length ? receipt.missing_sources.join('、') : '无' }}</span>
      </p>

      <table class="data-table">
        <thead>
          <tr>
            <th>来源标识</th>
            <th>分批状态</th>
            <th>分批编号</th>
            <th>记录数</th>
            <th>关联原始行ID</th>
            <th>说明</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="r in receipt.source_receipts" :key="r.source_id" :data-status="r.slot_status">
            <td>{{ r.source_id }}</td>
            <td>{{ r.slot_status }}</td>
            <td>{{ r.batch_no || '—' }}</td>
            <td>{{ r.record_count }}</td>
            <td>{{ r.raw_record_ids.join(', ') || '—' }}</td>
            <td>{{ r.reason || '—' }}</td>
            <td class="row-actions">
              <button
                v-if="r.slot_status === '接收失败'"
                class="link"
                type="button"
                @click="doRetry(r.source_id)"
              >
                重做该批
              </button>
            </td>
          </tr>
        </tbody>
      </table>

      <div class="page-actions">
        <button class="btn" type="button" data-testid="view-partial" @click="loadPartial">查看已接收/暂存原文</button>
        <button class="btn" type="button" data-testid="view-formal" @click="loadFormal">查看正式记录</button>
        <button class="btn ghost" type="button" data-testid="refresh-receipt" @click="loadReceipt">刷新回执</button>
      </div>

      <div v-if="partialView" class="sub-panel">
        <h4>缺路期间原文视图（尚缺：{{ partialView.missing_sources.join('、') || '无' }}）</h4>
        <p v-for="(logs, sid) in partialView.staged_pending_logs" :key="`staged-${sid}`">
          暂存待补交 · {{ sid }}：{{ logs.length }} 条
        </p>
        <table class="data-table">
          <thead>
            <tr><th>批号</th><th>来源</th><th>行号</th><th>原始正文</th></tr>
          </thead>
          <tbody>
            <tr v-for="row in partialView.received_raw_records" :key="String(row.id)">
              <td>{{ row['批次编号'] }}</td>
              <td>{{ row.source_id }}</td>
              <td>{{ row['原始行号'] }}</td>
              <td>{{ row['原始正文'] }}</td>
            </tr>
            <tr v-if="!partialView.received_raw_records.length">
              <td colspan="4" class="empty-state">暂无已接收原文</td>
            </tr>
          </tbody>
        </table>
      </div>

      <div v-if="formalView" class="sub-panel">
        <h4>正式记录</h4>
        <p v-if="!formalView.ok" class="error-text">
          {{ formalView.message }}（尚缺：{{ formalView.missing_sources?.join('、') }}）
        </p>
        <template v-else>
          <p>共 {{ formalView.record_count }} 条，按来源与行号排列：</p>
          <table class="data-table">
            <thead>
              <tr><th>批号</th><th>来源</th><th>行号</th><th>原始正文</th></tr>
            </thead>
            <tbody>
              <tr v-for="row in formalView.records" :key="String(row.id)">
                <td>{{ row['批次编号'] }}</td>
                <td>{{ row.source_id }}</td>
                <td>{{ row['原始行号'] }}</td>
                <td>{{ row['原始正文'] }}</td>
              </tr>
            </tbody>
          </table>
        </template>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { ref } from 'vue'

import {
  fetchFormalRecords,
  fetchGroupReceipt,
  fetchPartialRaw,
  retrySlot,
  submitGroup,
  supplementGroup,
} from '@/api/group-ingest'
import type {
  FormalRecords,
  GroupReceipt,
  GroupSourceInput,
  PartialRaw,
} from '@/api/group-ingest'

type SourceFormItem = {
  source_id: string
  logsText: string
}

const groupNo = ref('GRP-PLANT-20261121')
const timeWindow = ref('2026-11-21T08:00')
const sourceInputs = ref<SourceFormItem[]>([
  { source_id: 'plant-terminal-A', logsText: '' },
  { source_id: 'plant-terminal-B', logsText: '' },
])
const submitting = ref(false)
const errorMessage = ref('')
const receipt = ref<GroupReceipt | null>(null)
const partialView = ref<PartialRaw | null>(null)
const formalView = ref<FormalRecords | null>(null)

function addSource() {
  sourceInputs.value.push({ source_id: '', logsText: '' })
}

function removeSource(idx: number) {
  sourceInputs.value.splice(idx, 1)
}

function buildPayload(): GroupSourceInput[] {
  return sourceInputs.value
    .filter((item) => item.source_id.trim())
    .map((item) => ({
      source_id: item.source_id.trim(),
      raw_logs: item.logsText
        .split('\n')
        .map((line) => line.trim())
        .filter(Boolean)
        .map((body) => ({ body })),
    }))
}

async function submit() {
  errorMessage.value = ''
  partialView.value = null
  formalView.value = null
  submitting.value = true
  try {
    receipt.value = await submitGroup(groupNo.value, timeWindow.value, buildPayload())
    if (!receipt.value.ok) errorMessage.value = receipt.value.message
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '成组提交失败'
  } finally {
    submitting.value = false
  }
}

async function supplement() {
  errorMessage.value = ''
  partialView.value = null
  formalView.value = null
  submitting.value = true
  try {
    receipt.value = await supplementGroup(groupNo.value, buildPayload())
    if (!receipt.value.ok) errorMessage.value = receipt.value.message
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '补送失败'
  } finally {
    submitting.value = false
  }
}

async function loadReceipt() {
  errorMessage.value = ''
  try {
    receipt.value = await fetchGroupReceipt(groupNo.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回执读取失败'
  }
}

async function loadPartial() {
  formalView.value = null
  try {
    partialView.value = await fetchPartialRaw(groupNo.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '原文读取失败'
  }
}

async function loadFormal() {
  partialView.value = null
  try {
    formalView.value = await fetchFormalRecords(groupNo.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '正式记录读取失败'
  }
}

async function doRetry(sourceId: string) {
  errorMessage.value = ''
  try {
    const result = await retrySlot(groupNo.value, sourceId)
    if (!result.ok) {
      errorMessage.value = result.message
      return
    }
    await loadReceipt()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '重做失败'
  }
}
</script>

<style scoped>
.group-form {
  display: flex;
  flex-direction: column;
  gap: 14px;
  margin-bottom: 24px;
  padding: 16px;
  border: 1px solid var(--border-color, #d9dde3);
  border-radius: 8px;
  background: #fff;
}
.form-row {
  display: flex;
  gap: 16px;
  flex-wrap: wrap;
}
.source-picker {
  display: flex;
  flex-direction: column;
  gap: 10px;
}
.picker-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-weight: 600;
}
.source-row {
  display: flex;
  gap: 12px;
  align-items: flex-start;
  padding: 10px;
  border: 1px dashed #c4cad3;
  border-radius: 6px;
}
.logs-field {
  flex: 1;
}
.logs-field textarea {
  width: 100%;
  font-family: monospace;
}
.receipt-panel,
.sub-panel {
  margin-top: 16px;
  padding: 16px;
  border: 1px solid var(--border-color, #d9dde3);
  border-radius: 8px;
  background: #fff;
}
.sub-panel {
  border-style: dashed;
}
.link.danger {
  color: #c0392b;
  white-space: nowrap;
}
</style>
