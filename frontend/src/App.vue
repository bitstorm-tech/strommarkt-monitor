<script setup>
import { computed, reactive, ref, watch } from 'vue'

// "Alle" nur bis zu dieser Trefferzahl, sonst lädt der Browser Millionen Zeilen; muss zum Limit im Backend passen.
const MAX_PAGE_SIZE = 10000
// Standardmäßig sichtbare Spalten; `key` = Spaltenname der API (und Sortierschlüssel)
const standard = [
  { key: 'EinheitMastrNummer', label: 'MaStR-Nr.' },
  { key: 'NameStromerzeugungseinheit', label: 'Name' },
  { key: 'Energietraeger', label: 'Energieträger' },
  { key: 'EinheitBetriebsstatus', label: 'Betriebsstatus' },
  { key: 'Bruttoleistung', label: 'Bruttoleistung (kW)', type: 'num' },
  { key: 'Nettonennleistung', label: 'Nettonennleistung (kW)', type: 'num' },
  { key: 'Inbetriebnahmedatum', label: 'Inbetriebnahme', type: 'date' },
  { key: 'Registrierungsdatum', label: 'Registrierung', type: 'date' },
  { key: 'Bundesland', label: 'Bundesland' },
  { key: 'Postleitzahl', label: 'PLZ' },
  { key: 'Ort', label: 'Ort' },
  { key: 'Landkreis', label: 'Landkreis' },
]
// Suche plus ein Filter je sichtbarer Spalte: `Spalte.von`/`Spalte.bis` bei Zahlen und Daten, sonst `Spalte`.
const filter = reactive({ q: '' })
const view = reactive({ sort: '', desc: false, page: 0, size: 50 })
const options = ref({})
const result = ref({ total: 0, rows: [] })
const loading = ref(true)
const error = ref('')
// Alle Spalten der Datenbank; bis /api/spalten geantwortet hat nur die Standardspalten.
const allColumns = ref(standard)
const visible = ref(JSON.parse(localStorage.getItem('spalten')) ?? standard.map((c) => c.key))
// Reihenfolge wie ausgewählt: neu eingeblendete Spalten landen rechts.
const columns = computed(() => visible.value.map((k) => allColumns.value.find((c) => c.key === k)).filter(Boolean))
const columnSearch = ref('')
const choices = computed(() =>
  allColumns.value
    .filter((c) => c.label.toLowerCase().includes(columnSearch.value.trim().toLowerCase()))
    .sort((a, b) => a.label.localeCompare(b.label, 'de')),
)
const dialog = ref()
const pages = computed(() => Math.max(1, Math.ceil(result.value.total / view.size)))

async function getJson(url, signal) {
  const res = await fetch(url, { signal })
  if (!res.ok) throw new Error(`${res.status} ${await res.text()}`)
  return res.json()
}

let running, timer
async function load() {
  running?.abort() // eine ältere, langsamere Antwort darf die neuere nicht überschreiben
  const mine = (running = new AbortController())
  const params = new URLSearchParams({ page: view.page, page_size: view.size, desc: view.desc })
  if (view.sort) params.set('sort', view.sort)
  for (const [k, v] of Object.entries(filter)) if (v !== '' && v != null) params.set(k, v)
  loading.value = true
  try {
    result.value = await getJson(`/api/einheiten?${params}`, mine.signal)
    error.value = ''
  } catch (e) {
    if (e.name === 'AbortError') return
    error.value = `Daten konnten nicht geladen werden (${e.message}).`
  }
  if (mine === running) loading.value = false
}
const schedule = () => {
  clearTimeout(timer)
  timer = setTimeout(load, 250) // nicht bei jedem Tastendruck anfragen
}
watch(filter, () => {
  view.page = 0
  schedule()
})
watch(view, schedule)
watch(visible, (v) => {
  localStorage.setItem('spalten', JSON.stringify(v))
  if (!v.includes(view.sort)) view.sort = '' // nicht nach einer unsichtbaren Spalte sortieren
  // Filter ausgeblendeter Spalten entfernen, sonst filtern sie unsichtbar weiter.
  for (const k of Object.keys(filter)) if (k !== 'q' && !v.includes(k.split('.')[0])) delete filter[k]
})

function sortBy(key) {
  view.desc = view.sort === key && !view.desc
  view.sort = key
  view.page = 0
}
const ariaSort = (key) => (view.sort !== key ? 'none' : view.desc ? 'descending' : 'ascending')
const number = new Intl.NumberFormat('de-DE', { maximumFractionDigits: 3 })
function format(value, type) {
  if (value == null) return ''
  if (type === 'num') return number.format(value)
  if (type === 'date') return value.slice(0, 10).split('-').reverse().join('.')
  return value
}

getJson('/api/filter').then((o) => (options.value = o)).catch(() => {}) // Fehler meldet load() bereits
getJson('/api/spalten')
  // Für die übrigen Spalten gibt es keine gepflegten Titel: Tag-Namen an den Wortgrenzen trennen.
  .then((all) => (allColumns.value = all.map((c) => standard.find((s) => s.key === c.key) ?? { ...c, label: c.key.replace(/([a-z])([A-Z])/g, '$1 $2') })))
  .catch(() => {})
load()
</script>

<template>
  <header>
    <h1>Strommarkt-Monitor</h1>
    <p>Stromerzeugungseinheiten aus dem Marktstammdatenregister der Bundesnetzagentur</p>
  </header>

  <form class="filter" @submit.prevent @reset.prevent="Object.keys(filter).forEach((k) => (filter[k] = ''))">
    <label class="wide">Suche (Name oder MaStR-Nr.)
      <input v-model.trim="filter.q" type="search" placeholder="z. B. Solarpark oder SEE9…" />
    </label>
    <template v-for="c in columns" :key="c.key">
      <fieldset v-if="c.type === 'num' || c.type === 'date'">
        <legend>{{ c.label }}</legend>
        <input v-model="filter[c.key + '.von']" :type="c.type === 'num' ? 'number' : 'date'" step="any" placeholder="von" :aria-label="c.label + ' von'" />
        <input v-model="filter[c.key + '.bis']" :type="c.type === 'num' ? 'number' : 'date'" step="any" placeholder="bis" :aria-label="c.label + ' bis'" />
      </fieldset>
      <label v-else-if="options[c.key]">{{ c.label }}
        <select v-model="filter[c.key]">
          <option value="">Alle</option>
          <option v-for="o in options[c.key]" :key="o">{{ o }}</option>
        </select>
      </label>
      <label v-else>{{ c.label }} beginnt mit
        <input v-model.trim="filter[c.key]" />
      </label>
    </template>
    <button type="reset">Filter zurücksetzen</button>
  </form>

  <p v-if="error" class="error" role="alert">{{ error }}</p>

  <nav class="pager" aria-label="Seiten">
    <strong aria-live="polite">{{ number.format(result.total) }} Einheiten</strong>
    <span v-if="loading" class="loading">lädt …</span>
    <span class="spacer"></span>
    <label>Pro Seite
      <select v-model.number="view.size" @change="view.page = 0">
        <option v-for="n in [50, 100, 250]" :key="n" :value="n">{{ n }}</option>
        <option :value="MAX_PAGE_SIZE" :disabled="result.total > MAX_PAGE_SIZE">Alle{{ result.total > MAX_PAGE_SIZE ? ' (max. 10.000)' : '' }}</option>
      </select>
    </label>
    <button @click="dialog.showModal()">Einstellungen</button>
    <button :disabled="view.page === 0" @click="view.page--">‹ Zurück</button>
    <span>Seite {{ number.format(view.page + 1) }} von {{ number.format(pages) }}</span>
    <button :disabled="view.page + 1 >= pages" @click="view.page++">Weiter ›</button>
  </nav>

  <div class="table" :class="{ stale: loading }">
    <table>
      <thead>
        <tr>
          <th v-for="c in columns" :key="c.key" :aria-sort="ariaSort(c.key)" :class="c.type">
            <button @click="sortBy(c.key)">
              {{ c.label }} <span aria-hidden="true">{{ view.sort !== c.key ? '↕' : view.desc ? '↓' : '↑' }}</span>
            </button>
          </th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in result.rows" :key="row.EinheitMastrNummer">
          <td v-for="c in columns" :key="c.key" :class="c.type">{{ format(row[c.key], c.type) }}</td>
        </tr>
        <tr v-if="!result.rows.length && !loading">
          <td :colspan="columns.length" class="empty">Keine Einheiten für diese Filter gefunden.</td>
        </tr>
      </tbody>
    </table>
  </div>

  <dialog ref="dialog" aria-labelledby="einstellungen-titel">
    <form method="dialog">
      <h2 id="einstellungen-titel">Einstellungen</h2>
      <!-- Enter würde das Formular absenden und den Dialog schließen -->
      <input v-model="columnSearch" type="search" placeholder="Spalte suchen…" aria-label="Spalten durchsuchen" @keydown.enter.prevent />
      <fieldset>
        <legend>Sichtbare Spalten</legend>
        <div><label v-for="c in choices" :key="c.key"><input v-model="visible" type="checkbox" :value="c.key" /> {{ c.label }}</label></div>
      </fieldset>
      <div>
        <button type="button" @click="visible = standard.map((c) => c.key)">Standardspalten</button>
        <button>Schließen</button>
      </div>
    </form>
  </dialog>
</template>
