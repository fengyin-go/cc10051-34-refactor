import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export type DispatchResult = ActionResult & {
  [key: string]: unknown
}

async function postJson(url: string, body: Record<string, unknown> = {}): Promise<DispatchResult> {
  const response = await request(url, {
    method: 'POST',
    body: JSON.stringify(body),
  })
  return await response.json() as DispatchResult
}

async function getJson(url: string): Promise<DispatchResult> {
  const response = await request(url)
  if (!response.ok) throw new Error('分发登记查询失败')
  return await response.json() as DispatchResult
}

export async function fanoutPublishGroup(publishId: number): Promise<DispatchResult> {
  return postJson(`/api/dispatch/golden-publishes/${publishId}/fanout`)
}

export async function resumePublishGroup(publishId: number): Promise<DispatchResult> {
  return postJson(`/api/dispatch/golden-publishes/${publishId}/resume`)
}

export async function getPublishGroup(publishId: number): Promise<DispatchResult> {
  return getJson(`/api/dispatch/golden-publishes/${publishId}`)
}

export async function produceSubscriberDispatch(
  subscriberId: number,
  version: string,
): Promise<DispatchResult> {
  return postJson(`/api/dispatch/subscribers/${subscriberId}/produce`, { version })
}

export async function openDispatchRegistration(ref: string): Promise<DispatchResult> {
  const query = new URLSearchParams({ ref }).toString()
  return getJson(`/api/dispatch/open?${query}`)
}

export async function verifyDispatchByBatch(batchNo: string): Promise<DispatchResult> {
  const query = new URLSearchParams({ batch_no: batchNo }).toString()
  return getJson(`/api/dispatch/receipts/verify-batch?${query}`)
}

export async function signDispatchByBatch(batchNo: string): Promise<DispatchResult> {
  return postJson('/api/dispatch/sign-by-batch', { batch_no: batchNo })
}

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}
