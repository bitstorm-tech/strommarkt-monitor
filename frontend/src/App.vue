<script setup>
import { ref } from 'vue'
import Uebersicht from './Uebersicht.vue'

// Die Tabellen wie unter „Öffentliche Daten“ im MaStR. `api` = Ansicht im Backend; `standard` = standardmäßig
// sichtbare Spalten (`type` bei Zahlen und Daten, damit die Filter schon vor /api/…/spalten passen).
const tabs = [
  {
    api: 'einheiten',
    titel: 'Einheiten',
    suche: 'Name oder MaStR-Nr.',
    beispiel: 'z. B. Solarpark oder SEE9…',
    einheit: 'Einheiten',
    standard: [
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
    ],
  },
  {
    api: 'netzanschlusspunkte',
    titel: 'Netzanschlusspunkte und Lokationen',
    suche: 'MaStR-Nr. oder Bezeichnung',
    beispiel: 'z. B. SAN9… oder SEL9…',
    einheit: 'Einträge',
    standard: [
      { key: 'NetzanschlusspunktMastrNummer', label: 'MaStR-Nr.' },
      { key: 'NetzanschlusspunktBezeichnung', label: 'Netzanschlusspunktbezeichnung' },
      { key: 'LokationMaStRNummer', label: 'MaStR-Nr. der Lokation' },
      { key: 'NameDerTechnischenLokation', label: 'Name der technischen Lokation' },
      { key: 'Lokationtyp', label: 'Lokationstyp' },
      { key: 'VerknuepfteEinheitenMaStRNummern', label: 'MaStR-Nr. der Einheiten' },
      { key: 'NetzbetreiberName', label: 'Netzbetreiber' },
      { key: 'Spannungsebene', label: 'Spannungsebene' },
      { key: 'RegelzoneNetzanschlusspunkt', label: 'Regelzone' },
      { key: 'BilanzierungsgebietNetzanschlusspunktId', label: 'Y-EIC Bilanzierungsgebiet' },
      { key: 'Messlokation', label: 'Messlokation' },
      { key: 'Nettoengpassleistung', label: 'Nettoengpassleistung (kW)', type: 'num' },
      { key: 'NochInPlanung', label: 'Noch in Planung', type: 'num' },
    ],
  },
  {
    api: 'marktakteure',
    titel: 'Marktakteure',
    suche: 'Name oder MaStR-Nr.',
    beispiel: 'z. B. Stadtwerke oder SNB9…',
    einheit: 'Marktakteure',
    standard: [
      { key: 'MastrNummer', label: 'MaStR-Nr.' },
      { key: 'Firmenname', label: 'Name des Marktakteurs' },
      { key: 'Marktfunktion', label: 'Marktfunktion' },
      { key: 'Marktrollen', label: 'Marktrollen' },
      { key: 'Bundesland', label: 'Bundesland' },
      { key: 'Postleitzahl', label: 'PLZ' },
      { key: 'Ort', label: 'Ort' },
      { key: 'Strasse', label: 'Straße' },
      { key: 'Hausnummer', label: 'Hausnummer' },
      { key: 'Land', label: 'Land' },
      { key: 'RegistrierungsdatumMarktakteur', label: 'Registrierung', type: 'date' },
      { key: 'DatumLetzeAktualisierung', label: 'Letzte Aktualisierung', type: 'date' },
      { key: 'AcerCode', label: 'ACER-Code' },
      { key: 'Taetigkeitsbeginn', label: 'Tätigkeitsbeginn', type: 'date' },
      { key: 'Taetigkeitsende', label: 'Tätigkeitsende', type: 'date' },
    ],
  },
]
const aktiv = ref(tabs[0])
</script>

<template>
  <header>
    <h1>Strommarkt-Monitor</h1>
    <p>Öffentliche Daten aus dem Marktstammdatenregister der Bundesnetzagentur</p>
  </header>

  <div class="tabs" role="tablist">
    <button v-for="t in tabs" :key="t.api" :id="`tab-${t.api}`" role="tab" :aria-selected="t === aktiv" aria-controls="tabelle" @click="aktiv = t">
      {{ t.titel }}
    </button>
  </div>
  <!-- KeepAlive: Filter, Seite und Ergebnis jedes Tabs bleiben beim Wechseln erhalten -->
  <section id="tabelle" role="tabpanel" :aria-labelledby="`tab-${aktiv.api}`">
    <KeepAlive>
      <Uebersicht :key="aktiv.api" v-bind="aktiv" />
    </KeepAlive>
  </section>
</template>
