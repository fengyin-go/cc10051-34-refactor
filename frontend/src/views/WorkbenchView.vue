<template>
  <section class="page" :data-module="meta.key">
    <header class="page-head">
      <div>
        <h2>{{ meta.name }}</h2>
        <p class="page-desc">{{ meta.desc }}</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">
          登记{{ meta.entity }}
        </button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">
          导出{{ meta.name }}清单
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <section
      v-if="supportsDispatchTools"
      class="dispatch-console"
      data-testid="dispatch-console"
    >
      <header class="dispatch-head">
        <strong>分发登记凭据与旧编号查证</strong>
        <span>整组派送、单笔安排、分发任务和回执中心共用同一登记约定</span>
      </header>

      <div v-if="isSubscriberPage || isBatchLookupPage" class="dispatch-lookup">
        <input
          v-model="registrationRef"
          data-testid="registration-ref"
          type="text"
          placeholder="输入旧分发批次号或 reg- 登记凭据"
          @keyup.enter="lookupRegistration(isBatchLookupPage ? (meta.key === 'dist_receipt' ? 'verify' : 'sign') : 'open')"
        />
        <button
          class="btn"
          data-testid="open-registration"
          type="button"
          :disabled="dispatchBusy"
          @click="lookupRegistration('open')"
        >
          共用凭据打开
        </button>
        <button
          v-if="meta.key === 'dist_receipt'"
          class="btn primary"
          data-testid="verify-batch"
          type="button"
          :disabled="dispatchBusy"
          @click="lookupRegistration('verify')"
        >
          按旧号核对回执
        </button>
        <button
          v-if="meta.key === 'dist_task'"
          class="btn primary"
          data-testid="sign-batch"
          type="button"
          :disabled="dispatchBusy"
          @click="lookupRegistration('sign')"
        >
          按旧号签收
        </button>
      </div>

      <p v-if="dispatchMessage" class="dispatch-message" data-testid="dispatch-message">
        {{ dispatchMessage }}
      </p>

      <div
        v-if="dispatchResult"
        class="dispatch-result"
        data-testid="dispatch-result"
      >
        <div class="dispatch-summary">
          <span v-if="dispatchResult['分发批次'] || dispatchResult.batch_no">
            分发批次：{{ String(dispatchResult['分发批次'] ?? dispatchResult.batch_no) }}
          </span>
          <span v-if="dispatchResult['原始订阅方'] || dispatchResult.subscriber">
            原始收件方：{{ String(dispatchResult['原始订阅方'] ?? dispatchResult.subscriber) }}
          </span>
          <span v-if="dispatchResult['原发放版本'] || dispatchResult.version">
            原发放版本：{{ String(dispatchResult['原发放版本'] ?? dispatchResult.version) }}
          </span>
          <span v-if="dispatchResult['登记凭据'] || dispatchResult.credential">
            登记凭据：
            <button
              class="link"
              data-testid="use-credential"
              type="button"
              @click="openCredential(dispatchResult['登记凭据'] ?? dispatchResult.credential)"
            >
              {{ String(dispatchResult['登记凭据'] ?? dispatchResult.credential) }}
            </button>
          </span>
          <span v-if="dispatchResult.group_id || dispatchResult['整组编号']">
            整组编号：{{ String(dispatchResult['整组编号'] ?? dispatchResult.group_id) }}
            （{{ Number(dispatchResult.registered_count ?? 0) }}/{{ Number(dispatchResult.total ?? 0) }} 已登记）
          </span>
        </div>
        <details>
          <summary>查看完整提交回执与原申请/登记行区别</summary>
          <pre>{{ JSON.stringify(dispatchResult, null, 2) }}</pre>
        </details>
      </div>
    </section>

    <p class="status-legend">
      <span v-for="item in statusSummary" :key="item.status" class="legend-item">
        {{ item.status }}：{{ item.count }}
      </span>
    </p>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in meta.fields" :key="column">{{ column }}</th>
          <th>当前状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in meta.fields" :key="column">{{ row[column] ?? '—' }}</td>
          <td>{{ row.status }}</td>
          <td class="row-actions">
            <button
              v-for="action in meta.actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
            <button
              v-if="isPublishPage"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-整组派送`"
              :disabled="dispatchBusy"
              @click="handleFanout(row, false)"
            >
              整组派送
            </button>
            <button
              v-if="isPublishPage"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-续办整组`"
              :disabled="dispatchBusy"
              @click="handleFanout(row, true)"
            >
              续办并核对整组
            </button>
            <button
              v-if="isSubscriberPage"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-单笔安排`"
              :disabled="dispatchBusy"
              @click="openSingleArrange(row)"
            >
              单笔安排
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="meta.fields.length + 2" class="empty-state">
            暂无{{ meta.name }}数据，可先登记{{ meta.entity }}
          </td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记{{ meta.entity }}</h3>
        <label v-for="field in meta.fields" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <div v-if="singleTarget" class="modal-backdrop" data-testid="single-dispatch-modal">
      <form class="modal" @submit.prevent="submitSingleArrange">
        <h3>为「{{ String(singleTarget['订阅方标识'] ?? singleTarget.id) }}」安排单笔分发</h3>
        <label class="create-field">
          <span>发布版本</span>
          <input
            v-model="singleVersion"
            data-testid="single-dispatch-version"
            type="text"
            placeholder="填写版本标识"
            required
          />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="singleTarget = null">取消</button>
          <button
            class="btn primary"
            data-testid="submit-single-dispatch"
            type="submit"
            :disabled="dispatchBusy"
          >
            {{ dispatchBusy ? '提交中…' : '确认单笔安排' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条{{ meta.name }}记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  createEntry,
  downloadEntries,
  fanoutPublishGroup,
  listEntries,
  openDispatchRegistration,
  produceSubscriberDispatch,
  resumePublishGroup,
  runAction as applyAction,
  signDispatchByBatch,
  verifyDispatchByBatch,
} from '@/api/backend-service'
import { moduleMeta } from '@/data/modules'
import type { EntryRow } from '@/data/types'

const props = defineProps<{ moduleKey: string }>()
const meta = moduleMeta(props.moduleKey)
const rows = ref<EntryRow[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = meta.fields.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const dispatchBusy = ref(false)
const dispatchMessage = ref('')
const dispatchResult = ref<Record<string, unknown> | null>(null)
const registrationRef = ref('')
const singleVersion = ref('')
const singleTarget = ref<EntryRow | null>(null)

const supportsDispatchTools = computed(() =>
  ['golden_publish', 'subscriber_mgr', 'dist_task', 'dist_receipt'].includes(meta.key),
)
const isPublishPage = computed(() => meta.key === 'golden_publish')
const isSubscriberPage = computed(() => meta.key === 'subscriber_mgr')
const isBatchLookupPage = computed(() => ['dist_task', 'dist_receipt'].includes(meta.key))
const stats = computed(() => [
  { label: meta.metrics[0], value: total.value },
  { label: meta.metrics[1], value: rows.value.filter((row) => row.status === meta.statuses[1]).length },
  {
    label: meta.metrics[2],
    value: rows.value.filter((row) => row.status === meta.statuses[meta.statuses.length - 1]).length,
  },
])
const statusSummary = computed(() =>
  meta.statuses.map((status) => ({
    status,
    count: rows.value.filter((row) => String(row.status) === status).length,
  })),
)

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  try {
    await downloadEntries(meta.key, filters.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function presentDispatchResult(result: Record<string, unknown>) {
  dispatchResult.value = result
  dispatchMessage.value = String(
    result.message ?? (result.ok === false ? '分发登记未完成' : '分发登记已完成'),
  )
  if (result.ok !== false) void reload()
}

async function handleFanout(row: EntryRow, resume = false) {
  dispatchBusy.value = true
  dispatchMessage.value = ''
  try {
    const result = resume
      ? await resumePublishGroup(Number(row.id))
      : await fanoutPublishGroup(Number(row.id))
    presentDispatchResult(result)
  } catch (error) {
    dispatchMessage.value = error instanceof Error ? error.message : '整组派送失败'
  } finally {
    dispatchBusy.value = false
  }
}

async function openSingleArrange(row: EntryRow) {
  singleTarget.value = row
  singleVersion.value = ''
  dispatchMessage.value = ''
  try {
    const published = await listEntries('golden_publish', { status: '已发布' })
    const versionRow = published.items.find((item) => String(item['版本标识'] ?? '').trim())
    singleVersion.value = String(versionRow?.['版本标识'] ?? '')
  } catch {
    // 用户仍可手工输入版本标识。
  }
}

async function submitSingleArrange() {
  if (!singleTarget.value) return
  dispatchBusy.value = true
  try {
    const result = await produceSubscriberDispatch(
      Number(singleTarget.value.id),
      singleVersion.value.trim(),
    )
    presentDispatchResult(result)
    if (result.ok !== false) singleTarget.value = null
  } catch (error) {
    dispatchMessage.value = error instanceof Error ? error.message : '单笔安排失败'
  } finally {
    dispatchBusy.value = false
  }
}

async function lookupRegistration(action: 'open' | 'verify' | 'sign') {
  const ref = registrationRef.value.trim()
  if (!ref) {
    dispatchMessage.value = '请输入旧分发批次号或登记凭据'
    return
  }
  dispatchBusy.value = true
  try {
    const result = action === 'verify'
      ? await verifyDispatchByBatch(ref)
      : action === 'sign'
        ? await signDispatchByBatch(ref)
        : await openDispatchRegistration(ref)
    presentDispatchResult(result)
  } catch (error) {
    dispatchMessage.value = error instanceof Error ? error.message : '登记查证失败'
  } finally {
    dispatchBusy.value = false
  }
}

function openCredential(value: unknown) {
  registrationRef.value = String(value ?? '')
  void lookupRegistration('open')
}

function openCreate() {
  createValues.value = Object.fromEntries(meta.fields.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const result = await createEntry(meta.key, createValues.value)
    if (!result.ok) throw new Error(result.message)
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: EntryRow) {
  errorMessage.value = ''
  try {
    const result = await applyAction(meta.key, Number(row.id), action)
    if (!result.ok) throw new Error(result.message)
    const actionEntry = (result as { entry?: EntryRow }).entry
    const nested = actionEntry?.['最近整组回执'] ?? actionEntry?.['最近分发回执']
    if (nested && typeof nested === 'object') {
      presentDispatchResult(nested as Record<string, unknown>)
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await listEntries(meta.key, filters.value)
    rows.value = payload.items
    total.value = payload.total
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '业务列表读取失败'
  }
}

onMounted(reload)
</script>

<style scoped>
.dispatch-console {
  margin: 12px 0 16px;
  padding: 14px 16px;
  border: 1px solid #d9e2ef;
  border-radius: 12px;
  background: #f8fbff;
}

.dispatch-head {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  align-items: baseline;
  justify-content: space-between;
  margin-bottom: 10px;
}

.dispatch-head span {
  color: #64748b;
  font-size: 13px;
}

.dispatch-lookup {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.dispatch-lookup input {
  min-width: min(420px, 100%);
  flex: 1 1 280px;
  padding: 8px 10px;
  border: 1px solid #cbd5e1;
  border-radius: 8px;
}

.dispatch-message {
  margin: 10px 0 0;
  color: #b45309;
}

.dispatch-result {
  margin-top: 10px;
}

.dispatch-summary {
  display: flex;
  flex-direction: column;
  gap: 4px;
  padding: 10px 12px;
  border-radius: 8px;
  background: #fff;
  border: 1px solid #e2e8f0;
  font-size: 14px;
}

.dispatch-result pre {
  max-height: 280px;
  overflow: auto;
  margin: 8px 0 0;
  padding: 10px;
  border-radius: 8px;
  background: #0f172a;
  color: #e2e8f0;
  font-size: 12px;
}
</style>
