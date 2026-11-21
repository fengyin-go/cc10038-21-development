import { request } from './client'

/** 成组多源采集：多选来源一次提交、按源分批、成组回执与补送。 */

export type GroupSourceInput = {
  source_id: string
  raw_logs: { body: string }[]
}

export type SourceReceipt = {
  source_id: string
  slot_status: string
  batch_no: string
  record_count: number
  raw_record_ids: number[]
  reason: string
  idempotent?: boolean
}

export type PendingItem = {
  source_id: string
  batch_no: string
  status: string
  raw_record_ids: number[]
  reason: string
}

export type GroupReceipt = {
  ok: boolean
  message: string
  group_no: string
  group_status: string
  is_resubmit: boolean
  missing_sources: string[]
  formal_visible: boolean
  source_receipts: SourceReceipt[]
  pending_checklist: PendingItem[]
}

export type PartialRaw = {
  ok: boolean
  group_no: string
  missing_sources: string[]
  received_raw_records: Record<string, string | number | boolean>[]
  staged_pending_logs: Record<string, { body: string }[]>
}

export type FormalRecords = {
  ok: boolean
  message?: string
  formal_visible: boolean
  group_no?: string
  record_count?: number
  missing_sources?: string[]
  records?: Record<string, string | number | boolean>[]
}

async function post(path: string, body: unknown): Promise<unknown> {
  const response = await request(path, {
    method: 'POST',
    body: JSON.stringify(body),
  })
  return await response.json()
}

async function get(path: string): Promise<unknown> {
  const response = await request(path)
  if (!response.ok) throw new Error('成组采集数据读取失败')
  return await response.json()
}

export async function submitGroup(
  groupNo: string,
  timeWindow: string,
  sources: GroupSourceInput[],
): Promise<GroupReceipt> {
  return (await post('/api/group-ingest/submit', {
    group_no: groupNo,
    time_window: timeWindow,
    sources,
  })) as GroupReceipt
}

export async function supplementGroup(
  groupNo: string,
  sources: GroupSourceInput[],
): Promise<GroupReceipt> {
  return (await post('/api/group-ingest/supplement', {
    group_no: groupNo,
    sources,
  })) as GroupReceipt
}

export async function fetchGroupReceipt(groupNo: string): Promise<GroupReceipt> {
  return (await get(`/api/group-ingest/receipt/${encodeURIComponent(groupNo)}`)) as GroupReceipt
}

export async function fetchPartialRaw(groupNo: string): Promise<PartialRaw> {
  return (await get(`/api/group-ingest/partial/${encodeURIComponent(groupNo)}`)) as PartialRaw
}

export async function fetchFormalRecords(groupNo: string): Promise<FormalRecords> {
  return (await get(`/api/group-ingest/formal/${encodeURIComponent(groupNo)}`)) as FormalRecords
}

export async function retrySlot(groupNo: string, sourceId: string): Promise<{ ok: boolean; message: string }> {
  return (await post('/api/group-ingest/retry-slot', {
    group_no: groupNo,
    source_id: sourceId,
  })) as { ok: boolean; message: string }
}
