/**
 * Satelliittikuvat Web Viewer (OpenLayers Edition)
 * High-performance native COG rendering with hardware acceleration.
 */

// --- CONFIGURATION ---
const IMAGE_BASE_URL = "imagery/"; 
const INVENTORY_URL = IMAGE_BASE_URL + "visual/inventory.json";
const LEGENDS_URL = IMAGE_BASE_URL + "legends/legends.json";
const CONFIG_URL = "config.json";

// --- LAYER ORDERING (Z-Indices) ---
// Satellite imagery uses timestamp / 100000 (currently ~17 million).
// UI and Overlays must be significantly higher to stay on top.
const Z_INDEX_IDENTIFY = 100000000; // 100M
const Z_INDEX_HIGHLIGHT = 110000000; // 110M
const Z_INDEX_OVERLAYS = 200000000; // 200M

// --- UI DICTIONARY ---
const TRANSLATIONS = {
    "S1": { title: "SAR tutka", subtitle: "(ESA Sentinel 1)" },
    "S2": { title: "Optinen", subtitle: "(ESA Sentinel 2)" },
    "FUSED": { title: "Sensorifuusio", subtitle: "(S1 + S2 yhdistelmäkuvat)" },
    "VV": { title: "VV-polarisaatio", subtitle: "(Pystysuora lähetys ja vastaanotto)" },
    "VH": { title: "VH-polarisaatio", subtitle: "(Pysty-vaaka ristiinpolarisaatio)" },
    "RATIO": { title: "VV/VH-suhde", subtitle: "" },
    "AP": { title: "Ilmakehän läpäisy", subtitle: "(SWIR-väriyhdistelmä)" },
    "NDBI": { title: "NDBI-indeksi", subtitle: "(Rakennetun ympäristön indeksi)" },
    "NDBI_CLEAN": { title: "NDBI_CLEAN", subtitle: "(Häiriösuodatettu rakennetun ympäristön indeksi)" },
    "NDRE": { title: "NDRE-indeksi", subtitle: "(Kasvillisuuden punaisen reunan indeksi)" },
    "NDVI": { title: "NDVI-indeksi", subtitle: "(Normalisoitu kasvillisuusindeksi)" },
    "NIRFC": { title: "NIR-värivääräkuva", subtitle: "(Lähi-infrapunakooste)" },
    "TCI": { title: "TCI-kuva", subtitle: "(Luonnollisen värin kuva / Tosivärikuva)" },
    "NBR": { title: "NBR-indeksi", subtitle: "(Normalisoitu paloindeksi tai Paloalueindeksi)" },
    "LIFE-MACHINE": { title: "LIFE-MACHINE", subtitle: "(Biomassan ja keinotekoisten rakenteiden erottelu)" },
    "RADAR-BURN": { title: "RADAR-BURN", subtitle: "(Tutkaheijastumat optisen kuvan päällä)" },
    "TARGET-PROBE-V2": { title: "TARGET-PROBE-V2", subtitle: "(Kehittynyt kohde- ja rakennustunnistus)" }
};

const S2_PRIORITY = ["TCI", "NIRFC", "AP", "NDBI_CLEAN", "NDBI", "NDRE", "NDVI", "NBR", "CAMO"];

// --- HELPERS ---
function formatSize(bytes) {
    if (!bytes || bytes === 0) return "0 t";
    const k = 1024;
    const sizes = ['t', 'kt', 'Mt', 'Gt', 'Tt'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + sizes[i];
}

function downloadFile(url, filename) {
    const link = document.createElement('a');
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
}

// --- GLOBAL STATE ---
let map;
let activeLayers = {}; // path -> {layer, meta}
let hoverSource;
let highlightSource;
let inventoryData = [];
let s2SortMode = 'product'; // 'product' or 'grid'
let identifyOpticalLayer;
let identifyRadarLayer;
let masterLegends = {}; 
let sentinelAttribution = new ol.source.Vector({
    attributions: '' // Starts empty
});

const baseLayers = {
    'dark': new ol.layer.Tile({
        source: new ol.source.XYZ({
            url: 'https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}',
            attributions: 'Tiles &copy; Esri',
            maxZoom: 16
        }),
        visible: true
    }),
    'osm': new ol.layer.Tile({
        source: new ol.source.OSM(),
        visible: false
    })
};

// --- INITIALIZATION ---
document.addEventListener('DOMContentLoaded', () => {
    initMap();
    initBasePicker();
    loadConfig();
    loadInventory();
    loadLegends();
    checkLogo();

    // Action buttons
    const deselectBtn = document.getElementById('deselect-all');
    if (deselectBtn) deselectBtn.onclick = deselectAllLayers;

    const zoomBtn = document.getElementById('zoom-available');
    if (zoomBtn) zoomBtn.onclick = zoomToAvailable;

    const optBtn = document.getElementById('identify-optical');
    if (optBtn) optBtn.onclick = toggleIdentifyOptical;

    const radBtn = document.getElementById('identify-radar');
    if (radBtn) radBtn.onclick = toggleIdentifyRadar;
});

function deselectAllLayers() {
    const checkboxes = document.querySelectorAll('#layer-picker input[type="checkbox"]');
    checkboxes.forEach(chk => {
        if (chk.checked) {
            chk.checked = false;
            // Trigger the change manually to run toggleLayer
            const event = new Event('change');
            chk.dispatchEvent(event);
        }
    });
}

function updateAcquisitionRange(layers) {
    const rangeEl = document.getElementById('acq-range');
    if (!rangeEl || !layers || layers.length === 0) return;

    const times = layers
        .map(l => l.acquisition_time)
        .filter(t => t && t !== "Unknown")
        .sort();

    if (times.length > 0) {
        const start = times[0];
        const end = times[times.length - 1];
        rangeEl.innerText = `(${start} - ${end})`;
    }
}

function updateGroupMarkers() {
    // Update Product Groups (e.g. TCI, NDVI)
    const prodGroups = document.querySelectorAll('.prod-group');
    prodGroups.forEach(group => {
        const hasActive = group.querySelectorAll('input:checked').length > 0;
        group.classList.toggle('has-active', hasActive);
    });

    // Update Grid Groups
    const gridGroups = document.querySelectorAll('.grid-group');
    gridGroups.forEach(group => {
        const hasActive = group.querySelectorAll('input:checked').length > 0;
        group.classList.toggle('has-active', hasActive);
    });

    // Update Main Categories (e.g. S1, S2, FUSED)
    const satGroups = document.querySelectorAll('.sat-group');
    satGroups.forEach(group => {
        const hasActive = group.querySelectorAll('input:checked').length > 0;
        group.classList.toggle('has-active', hasActive);
    });
}

function initMap() {
    // OpenLayers Map initialization
    map = new ol.Map({
        target: 'map',
        controls: ol.control.defaults.defaults().extend([
            new ol.control.Attribution({
                collapsible: false
            })
        ]),
        layers: [
            baseLayers.dark,
            baseLayers.osm
        ],
        view: new ol.View({
            center: ol.proj.fromLonLat([24.9384, 60.1699]),
            zoom: 8
        })
    });

    // Dummy layer to hold the dynamic Sentinel attribution
    const attrLayer = new ol.layer.Vector({
        source: sentinelAttribution
    });
    map.addLayer(attrLayer);

    // Scale Line
    map.addControl(new ol.control.ScaleLine({ units: 'metric' }));

    // Hover Preview Source & Layer (Sidebar hover)
    hoverSource = new ol.source.Vector();
    const hoverLayer = new ol.layer.Vector({
        source: hoverSource,
        zIndex: Z_INDEX_HIGHLIGHT,
        style: new ol.style.Style({
            stroke: new ol.style.Stroke({ color: '#00bcd4', width: 3 }),
            fill: new ol.style.Fill({ color: 'rgba(0, 188, 212, 0.1)' })
        })
    });
    map.addLayer(hoverLayer);

    // Highlight Source & Layer (Map hover on identify layers)
    highlightSource = new ol.source.Vector();
    const highlightLayer = new ol.layer.Vector({
        source: highlightSource,
        zIndex: Z_INDEX_HIGHLIGHT,
        style: (feature) => {
            const isOptical = feature.get('isOptical');
            return new ol.style.Style({
                stroke: new ol.style.Stroke({ color: '#ffeb3b', width: 3 }),
                fill: new ol.style.Fill({ color: 'rgba(255, 235, 59, 0.2)' }),
                text: new ol.style.Text({
                    text: feature.get('label'),
                    font: isOptical ? 'bold 14px sans-serif' : '11px sans-serif',
                    fill: new ol.style.Fill({ color: '#ffeb3b' }),
                    stroke: new ol.style.Stroke({ color: '#000', width: 3 })
                })
            });
        }
    });
    map.addLayer(highlightLayer);

    // Pointer move listener for identify highlights
    map.on('pointermove', (evt) => {
        if (evt.dragging) return;
        
        highlightSource.clear();
        const pixel = map.getEventPixel(evt.originalEvent);
        const feature = map.forEachFeatureAtPixel(pixel, (f, layer) => {
            if (layer === identifyOpticalLayer || layer === identifyRadarLayer) {
                return f;
            }
        });

        if (feature) {
            const isOptical = identifyOpticalLayer && identifyOpticalLayer.getSource().getFeatures().includes(feature);
            const clone = feature.clone();
            clone.set('isOptical', isOptical);
            highlightSource.addFeature(clone);
            map.getTargetElement().style.cursor = 'pointer';
        } else {
            map.getTargetElement().style.cursor = '';
        }
    });

    // Click listener for identify features
    map.on('singleclick', (evt) => {
        const pixel = map.getEventPixel(evt.originalEvent);
        const feature = map.forEachFeatureAtPixel(pixel, (f, layer) => {
            if (layer === identifyOpticalLayer || layer === identifyRadarLayer) return f;
        });

        if (feature) {
            if (feature.get('isOptical')) {
                jumpToSidebar('S2', 'TCI', feature.get('label'));
            } else if (feature.get('isRadar')) {
                jumpToSidebar('S1', 'RATIO', feature.get('time'));
            }
        }
    });
}

function jumpToSidebar(sat, prod, identifier) {
    const group = document.getElementById(`group-${sat}`);
    if (group) group.classList.remove('collapsed');

    if (sat === 'S2' && s2SortMode === 'grid') {
        const gridGroup = document.getElementById(`grid-S2-${identifier}`);
        if (gridGroup) {
            gridGroup.classList.remove('collapsed');
            gridGroup.scrollIntoView({ behavior: 'smooth', block: 'center' });
            gridGroup.classList.add('jump-highlight');
            setTimeout(() => gridGroup.classList.remove('jump-highlight'), 2000);
        }
        return;
    }

    const prodGroup = document.getElementById(`prod-${sat}-${prod}`);
    if (prodGroup) {
        prodGroup.classList.remove('collapsed');
        
        // Find the layer item
        let target;
        if (sat === 'S2') {
            target = prodGroup.querySelector(`.layer-item[data-grid="${identifier}"]`);
        } else {
            target = prodGroup.querySelector(`.layer-item[data-time="${identifier}"]`);
        }
        
        if (target) {
            target.scrollIntoView({ behavior: 'smooth', block: 'center' });
            // Highlight the target temporarily
            target.classList.add('jump-highlight');
            setTimeout(() => target.classList.remove('jump-highlight'), 2000);
        } else {
            // If specific layer not found, just scroll to the product group
            prodGroup.scrollIntoView({ behavior: 'smooth', block: 'center' });
        }
    }
}

function initBasePicker() {
    const container = document.getElementById('base-picker');
    const options = [
        { id: 'dark', label: 'Tumma' },
        { id: 'osm', label: 'Kartta' }
    ];

    options.forEach(opt => {
        const btn = document.createElement('button');
        btn.className = 'base-btn' + (opt.id === 'dark' ? ' active' : '');
        btn.innerText = opt.label;
        btn.onclick = () => {
            Object.keys(baseLayers).forEach(key => {
                baseLayers[key].setVisible(key === opt.id);
            });
            container.querySelectorAll('.base-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
        };
        container.appendChild(btn);
    });
}

async function checkLogo() {
    try {
        const resp = await fetch('logo.png', { method: 'HEAD' });
        if (resp.ok) document.getElementById('logo-container').style.display = 'block';
    } catch (e) {}
}

async function loadConfig() {
    try {
        const resp = await fetch(CONFIG_URL);
        if (resp.ok) {
            const config = await resp.json();
            if (config.overlays && Array.isArray(config.overlays)) {
                loadOverlays(config.overlays);
            }
        }
    } catch (e) {
        console.warn("Could not load config:", e);
    }
}

function loadOverlays(configs) {
    configs.forEach((cfg, index) => {
        const isObject = typeof cfg === 'object' && cfg !== null;
        const url = isObject ? cfg.url : cfg;
        
        // Style defaults
        const color = (isObject && cfg.color) ? cfg.color : '#ffeb3b';
        const width = (isObject && cfg.lineWidth) ? cfg.lineWidth : 2.5;
        const markerSize = (isObject && cfg.markerSize) ? cfg.markerSize : 6;
        
        let lineDash = null;
        if (isObject && cfg.lineStyle) {
            if (cfg.lineStyle === 'dashed') lineDash = [10, 10];
            else if (cfg.lineStyle === 'dotted') lineDash = [2, 7];
        }

        const source = new ol.source.Vector({
            url: url,
            format: new ol.format.GeoJSON()
        });

        const layer = new ol.layer.Vector({
            source: source,
            zIndex: Z_INDEX_OVERLAYS + index,
            style: function(feature) {
                const geometry = feature.getGeometry();
                const type = geometry.getType();
                
                if (type === 'Point' || type === 'MultiPoint') {
                    const label = feature.get('label') || feature.get('name') || feature.get('id') || '';
                    return new ol.style.Style({
                        image: new ol.style.Circle({
                            radius: markerSize,
                            fill: new ol.style.Fill({ color: color }),
                            stroke: new ol.style.Stroke({ color: '#000', width: 2 })
                        }),
                        text: new ol.style.Text({
                            text: label,
                            font: 'bold 13px sans-serif',
                            fill: new ol.style.Fill({ color: '#fff' }),
                            stroke: new ol.style.Stroke({ color: '#000', width: 3 }),
                            offsetY: -(markerSize + 10),
                            overflow: true
                        })
                    });
                } else {
                    // Polygons and Multipolygons (and LineStrings) - Outline only
                    return new ol.style.Style({
                        stroke: new ol.style.Stroke({
                            color: color,
                            width: width,
                            lineDash: lineDash
                        }),
                        fill: new ol.style.Fill({
                            color: 'rgba(255, 235, 59, 0)' // Invisible fill
                        })
                    });
                }
            }
        });
        map.addLayer(layer);
    });
}

async function loadLegends() {
    try {
        const url = window.location.origin + window.location.pathname.replace('index.html', '') + LEGENDS_URL;
        const response = await fetch(url);
        if (response.ok) masterLegends = await response.json();
    } catch (e) {}
}

async function loadInventory() {
    const picker = document.getElementById('layer-picker');
    const progressBar = document.getElementById('progress-bar');
    const loadingText = document.getElementById('loading-text');

    try {
        const response = await fetch(INVENTORY_URL);
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);

        const contentLength = response.headers.get('content-length');
        const total = parseInt(contentLength, 10);
        let loaded = 0;

        const reader = response.body.getReader();
        const chunks = [];
        
        while(true) {
            const {done, value} = await reader.read();
            if (done) break;
            chunks.push(value);
            loaded += value.length;
            if (total) {
                const percent = Math.round((loaded / total) * 50); // First 50% for download
                progressBar.style.width = `${percent}%`;
                loadingText.innerText = `Haetaan... ${Math.round((loaded / 1024 / 1024))} Mt`;
            }
        }

        loadingText.innerText = "Käsitellään tietoja...";
        const allChunks = new Uint8Array(loaded);
        let position = 0;
        for (const chunk of chunks) {
            allChunks.set(chunk, position);
            position += chunk.length;
        }

        const decoder = new TextDecoder("utf-8");
        const jsonString = decoder.decode(allChunks);
        const data = JSON.parse(jsonString);

        if (data.layers && data.layers.length > 0) {
            inventoryData = data.layers;
            updateAcquisitionRange(data.layers);
            renderLayerPicker(data.layers);
        } else {
            picker.innerHTML = `<div id="loading">Ei kuvia saatavilla.</div>`;
        }
    } catch (e) {
        console.error("Inventory error:", e);
        picker.innerHTML = `<div id="loading">Virhe ladattaessa inventaariota: ${e.message}</div>`;
    }
}

function zoomToAvailable() {
    if (!inventoryData || inventoryData.length === 0) return;
    let extent = ol.extent.createEmpty();
    inventoryData.forEach(layer => {
        const b = layer.bounds;
        if (!b) return;
        const layerExtent = ol.extent.boundingExtent([
            ol.proj.fromLonLat([b[0][1], b[0][0]]),
            ol.proj.fromLonLat([b[1][1], b[1][0]])
        ]);
        ol.extent.extend(extent, layerExtent);
    });
    if (!ol.extent.isEmpty(extent)) {
        map.getView().fit(extent, { padding: [50, 50, 50, 50], duration: 1000 });
    }
}

function toggleIdentifyOptical() {
    const btn = document.getElementById('identify-optical');
    if (identifyOpticalLayer) {
        map.removeLayer(identifyOpticalLayer);
        identifyOpticalLayer = null;
        if (highlightSource) highlightSource.clear();
        btn.classList.remove('active');
        return;
    }

    const source = new ol.source.Vector();
    const grids = {};

    inventoryData.forEach(layer => {
        if (!layer.product.startsWith('S2')) return;
        const grid = getGridSquare(layer);
        if (!grid) return;
        // Prefer TCI for the 'canonical' layer for the grid if possible
        if (!grids[grid] || layer.product === 'S2-TCI') {
            grids[grid] = layer;
        }
    });

    Object.keys(grids).forEach(gridId => {
        const layer = grids[gridId];
        let feature;
        if (layer.footprint) {
            const format = new ol.format.GeoJSON();
            feature = format.readFeature(layer.footprint, {
                dataProjection: 'EPSG:4326',
                featureProjection: 'EPSG:3857'
            });
        } else {
            const b = layer.bounds;
            const poly = new ol.geom.Polygon([[
                ol.proj.fromLonLat([b[0][1], b[0][0]]),
                ol.proj.fromLonLat([b[1][1], b[0][0]]),
                ol.proj.fromLonLat([b[1][1], b[1][0]]),
                ol.proj.fromLonLat([b[0][1], b[1][0]]),
                ol.proj.fromLonLat([b[0][1], b[0][0]])
            ]]);
            feature = new ol.Feature(poly);
        }
        feature.set('label', gridId);
        feature.set('isOptical', true);
        source.addFeature(feature);
    });

    identifyOpticalLayer = new ol.layer.Vector({
        source: source,
        zIndex: Z_INDEX_IDENTIFY,
        style: (feature) => new ol.style.Style({
            stroke: new ol.style.Stroke({ color: '#3f51b5', width: 2 }),
            fill: new ol.style.Fill({ color: 'rgba(63, 81, 181, 0.05)' }),
            text: new ol.style.Text({
                text: feature.get('label'),
                font: 'bold 14px sans-serif',
                fill: new ol.style.Fill({ color: '#3f51b5' }),
                stroke: new ol.style.Stroke({ color: '#fff', width: 2 })
            })
        })
    });
    map.addLayer(identifyOpticalLayer);
    btn.classList.add('active');
}

function toggleIdentifyRadar() {
    const btn = document.getElementById('identify-radar');
    if (identifyRadarLayer) {
        map.removeLayer(identifyRadarLayer);
        identifyRadarLayer = null;
        if (highlightSource) highlightSource.clear();
        btn.classList.remove('active');
        return;
    }

    const source = new ol.source.Vector();
    const seen = new Set();

    inventoryData.forEach(layer => {
        if (!layer.product.startsWith('S1')) return;
        if (seen.has(layer.acquisition_time)) return;
        seen.add(layer.acquisition_time);
        
        let feature;
        if (layer.footprint) {
            const format = new ol.format.GeoJSON();
            feature = format.readFeature(layer.footprint, {
                dataProjection: 'EPSG:4326',
                featureProjection: 'EPSG:3857'
            });
        } else {
            const b = layer.bounds;
            const poly = new ol.geom.Polygon([[
                ol.proj.fromLonLat([b[0][1], b[0][0]]),
                ol.proj.fromLonLat([b[1][1], b[0][0]]),
                ol.proj.fromLonLat([b[1][1], b[1][0]]),
                ol.proj.fromLonLat([b[0][1], b[1][0]]),
                ol.proj.fromLonLat([b[0][1], b[0][0]])
            ]]);
            feature = new ol.Feature(poly);
        }
        
        const date = new Date(layer.acquisition_time);
        const label = date.toLocaleString('en-GB', { 
            month: 'short', day: 'numeric', 
            hour: '2-digit', minute: '2-digit', timeZone: 'UTC' 
        }) + "Z";
        
        feature.set('label', label);
        feature.set('time', layer.acquisition_time);
        feature.set('isRadar', true);
        source.addFeature(feature);
    });

    identifyRadarLayer = new ol.layer.Vector({
        source: source,
        zIndex: Z_INDEX_IDENTIFY + 1,
        style: (feature) => new ol.style.Style({
            stroke: new ol.style.Stroke({ color: '#3f51b5', width: 2 }),
            fill: new ol.style.Fill({ color: 'rgba(63, 81, 181, 0.05)' }),
            text: new ol.style.Text({
                text: feature.get('label'),
                font: '11px sans-serif',
                fill: new ol.style.Fill({ color: '#3f51b5' }),
                stroke: new ol.style.Stroke({ color: '#fff', width: 2 }),
                overflow: true
            })
        })
    });
    map.addLayer(identifyRadarLayer);
    btn.classList.add('active');
}

function getGridSquare(layer) {
    if (!layer.product.startsWith("S2")) return "";
    const parts = layer.path.split('/');
    const filename = parts[parts.length - 1];
    return filename.startsWith('T') ? filename.split('-')[0] : "";
}

function setS2SortMode(mode) {
    if (s2SortMode === mode) return;
    s2SortMode = mode;
    renderLayerPicker(inventoryData);
}

function renderLayerPicker(layers) {
    const picker = document.getElementById('layer-picker');
    const progressBar = document.getElementById('progress-bar');
    const loadingText = document.getElementById('loading-text');

    const satOrder = ['S2', 'S1', 'FUSED'];
    const expandedSats = new Set();
    satOrder.forEach(sat => {
        const oldGroup = document.getElementById(`group-${sat}`);
        if (oldGroup && !oldGroup.classList.contains('collapsed')) {
            expandedSats.add(sat);
        }
    });

    picker.innerHTML = ''; 

    const groups = {};
    layers.forEach(layer => {
        const sat = layer.product.split('-')[0];
        const type = layer.product.split('-').slice(1).join('-');
        if (!groups[sat]) groups[sat] = {};
        if (!groups[sat][type]) groups[sat][type] = [];
        groups[sat][type].push(layer);
    });

    let totalLayers = layers.length;
    let renderedLayers = 0;

    satOrder.forEach(sat => {
        if (!groups[sat]) return;
        const satMeta = TRANSLATIONS[sat] || { title: sat, subtitle: "" };
        const satDiv = document.createElement('div');
        satDiv.className = 'sat-group' + (expandedSats.has(sat) ? '' : ' collapsed');
        satDiv.id = `group-${sat}`;
        
        let headerHtml = `
            <div class="sat-title" onclick="this.parentElement.classList.toggle('collapsed')">
                <span>${satMeta.title} <small style="text-transform: none; font-weight: normal; opacity: 0.7;">${satMeta.subtitle}</small></span>
            </div>
        `;

        if (sat === 'S2') {
            headerHtml += `
                <div class="sort-row">
                    <button class="sort-btn ${s2SortMode === 'product' ? 'active' : ''}" onclick="event.stopPropagation(); setS2SortMode('product')">Tuoteittain</button>
                    <button class="sort-btn ${s2SortMode === 'grid' ? 'active' : ''}" onclick="event.stopPropagation(); setS2SortMode('grid')">Ruuduittain</button>
                </div>
            `;
        }

        headerHtml += `<div class="prod-container"></div>`;
        satDiv.innerHTML = headerHtml;
        const prodContainer = satDiv.querySelector('.prod-container');

        if (sat === 'S2' && s2SortMode === 'grid') {
            // Re-group by grid
            const gridGroups = {};
            Object.keys(groups[sat]).forEach(type => {
                groups[sat][type].forEach(layer => {
                    const grid = getGridSquare(layer) || "Tuntematon";
                    if (!gridGroups[grid]) gridGroups[grid] = {};
                    if (!gridGroups[grid][type]) gridGroups[grid][type] = [];
                    gridGroups[grid][type].push(layer);
                });
            });

            const grids = Object.keys(gridGroups).sort();
            grids.forEach(grid => {
                const gridDiv = document.createElement('div');
                gridDiv.className = 'grid-group collapsed';
                gridDiv.id = `grid-S2-${grid}`;
                gridDiv.innerHTML = `
                    <div class="grid-title" onclick="this.parentElement.classList.toggle('collapsed')">
                        ${grid}
                    </div>
                    <div class="prod-container"></div>
                `;

                const gridTitle = gridDiv.querySelector('.grid-title');
                gridTitle.onmouseenter = () => {
                    // Show outline of a representative layer in this grid
                    const types = Object.keys(gridGroups[grid]);
                    let repLayer = null;
                    if (gridGroups[grid]['TCI']) {
                        repLayer = gridGroups[grid]['TCI'][0];
                    } else if (types.length > 0) {
                        repLayer = gridGroups[grid][types[0]][0];
                    }
                    if (repLayer) showLayerHover(repLayer);
                };
                gridTitle.onmouseleave = () => hoverSource.clear();

                const gridProdContainer = gridDiv.querySelector('.prod-container');

                const types = Object.keys(gridGroups[grid]).sort((a, b) => S2_PRIORITY.indexOf(a) - S2_PRIORITY.indexOf(b));
                types.forEach(type => {
                    const typeMeta = TRANSLATIONS[type] || { title: type, subtitle: "" };
                    const typeDiv = document.createElement('div');
                    typeDiv.className = 'prod-group collapsed';
                    typeDiv.id = `prod-S2-${grid}-${type}`;
                    typeDiv.innerHTML = `
                        <div class="prod-title" onclick="this.parentElement.classList.toggle('collapsed')">
                            ${typeMeta.title}
                        </div>
                        <div class="layer-container"></div>
                    `;
                    const layerContainer = typeDiv.querySelector('.layer-container');
                    
                    const sortedLayers = gridGroups[grid][type].sort((a, b) => b.acquisition_time.localeCompare(a.acquisition_time));
                    sortedLayers.forEach(layer => {
                        layerContainer.appendChild(createLayerItem(layer));
                        renderedLayers++;
                        const percent = 50 + Math.round((renderedLayers / totalLayers) * 50);
                        if (progressBar) progressBar.style.width = `${percent}%`;
                    });
                    gridProdContainer.appendChild(typeDiv);
                });
                prodContainer.appendChild(gridDiv);
            });
        } else {
            const types = Object.keys(groups[sat]).sort((a, b) => {
                if (sat === 'S2') return S2_PRIORITY.indexOf(a) - S2_PRIORITY.indexOf(b);
                return a.localeCompare(b);
            });

            types.forEach(type => {
                const typeMeta = TRANSLATIONS[type] || { title: type, subtitle: "" };
                const typeDiv = document.createElement('div');
                typeDiv.className = 'prod-group collapsed';
                typeDiv.id = `prod-${sat}-${type}`;
                typeDiv.innerHTML = `
                    <div class="prod-title" onclick="this.parentElement.classList.toggle('collapsed')">
                        ${typeMeta.title}
                        <span class="subtitle">${typeMeta.subtitle}</span>
                    </div>
                    <div class="layer-container"></div>
                `;
                const layerContainer = typeDiv.querySelector('.layer-container');

                const sortedLayers = groups[sat][type].sort((a, b) => {
                    if (sat === "S2") {
                        const gridA = getGridSquare(a), gridB = getGridSquare(b);
                        if (gridA !== gridB) return gridA.localeCompare(gridB);
                    }
                    return b.acquisition_time.localeCompare(a.acquisition_time);
                });

                sortedLayers.forEach(layer => {
                    layerContainer.appendChild(createLayerItem(layer));
                    renderedLayers++;
                    const percent = 50 + Math.round((renderedLayers / totalLayers) * 50);
                    if (progressBar) progressBar.style.width = `${percent}%`;
                });
                prodContainer.appendChild(typeDiv);
            });
        }
        picker.appendChild(satDiv);
    });
}

function showLayerHover(layer) {
    if (layer.footprint) {
        // Use precise footprint if available
        const format = new ol.format.GeoJSON();
        const feature = format.readFeature(layer.footprint, {
            dataProjection: 'EPSG:4326',
            featureProjection: 'EPSG:3857'
        });
        hoverSource.addFeature(feature);
    } else {
        // Fallback to bounds
        const b = layer.bounds; // [[lat, lon], [lat, lon]]
        const poly = new ol.geom.Polygon([[
            ol.proj.fromLonLat([b[0][1], b[0][0]]),
            ol.proj.fromLonLat([b[1][1], b[0][0]]),
            ol.proj.fromLonLat([b[1][1], b[1][0]]),
            ol.proj.fromLonLat([b[0][1], b[1][0]]),
            ol.proj.fromLonLat([b[0][1], b[0][0]])
        ]]);
        hoverSource.addFeature(new ol.Feature(poly));
    }
}

function createLayerItem(layer) {
    const div = document.createElement('div');
    div.className = 'layer-item';
    const grid = getGridSquare(layer);
    if (grid) div.dataset.grid = grid;
    div.dataset.time = layer.acquisition_time;
    
    const date = new Date(layer.acquisition_time);
    const friendlyTime = date.toLocaleString('en-GB', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', timeZone: 'UTC' }) + "Z";
    const sizeStr = formatSize(layer.file_size_bytes);

    div.innerHTML = `
        <input type="checkbox" id="chk-${layer.path}">
        <div class="layer-info">
            <span class="layer-time">${grid ? grid + ", " : ""}${friendlyTime}</span>
            <span class="layer-status">${layer.acquisition_time.split('T')[0]}</span>
        </div>
        <div class="layer-actions">
            <button class="dl-btn" title="Lataa täysi TIF">
                <svg viewBox="0 0 24 24" width="16" height="16">
                    <path fill="currentColor" d="M12 16l-5-5h3V4h4v7h3l-5 5zm9 2v2H3v-2h18z"/>
                </svg>
            </button>
            <span class="file-size">${sizeStr}</span>
        </div>
    `;

    div.onclick = (e) => {
        if (e.target.closest('.dl-btn')) {
            e.stopPropagation();
            const baseUrl = window.location.href.split('index.html')[0].split('?')[0];
            const url = baseUrl + IMAGE_BASE_URL + layer.path;
            const filename = layer.path.split('/').pop();
            downloadFile(url, filename);
            return;
        }
        
        if (e.target.tagName !== 'INPUT') {
            const chk = div.querySelector('input');
            chk.checked = !chk.checked;
            toggleLayer(layer, chk.checked, div);
        }
    };
    div.querySelector('input').onchange = (e) => toggleLayer(layer, e.target.checked, div);

    div.onmouseenter = () => {
        if (div.querySelector('input').checked) return;
        showLayerHover(layer);
    };
    div.onmouseleave = () => hoverSource.clear();

    return div;
}

function updateAttributions() {
    const years = new Set();
    Object.values(activeLayers).forEach(obj => {
        if (obj.layer.getVisible()) {
            const year = obj.meta.acquisition_time.split('-')[0];
            years.add(year);
        }
    });

    if (years.size === 0) {
        sentinelAttribution.setAttributions([]);
        return;
    }

    let attr = "Made with Copernicus Sentinel Data";
    const sortedYears = Array.from(years).sort();
    const yearStr = sortedYears.length > 1 
        ? `${sortedYears[0]}-${sortedYears[sortedYears.length - 1]}` 
        : sortedYears[0];
    attr += ` ${yearStr}`;
    
    sentinelAttribution.setAttributions([attr]);
}

function updateLegends() {
    updateAttributions();
    const panel = document.getElementById('legend-panel');
    panel.innerHTML = '';
    const activeLegendIds = new Set();
    Object.values(activeLayers).forEach(obj => {
        // Only show legends for layers that are actually visible
        if (obj.layer.getVisible() && obj.meta.legend_id) {
            activeLegendIds.add(obj.meta.legend_id);
        }
    });
    
    activeLegendIds.forEach(id => {
        if (masterLegends[id]) {
            // Find one of the active layers with this legend_id to get resolution
            const sample = Object.values(activeLayers).find(o => o.meta.legend_id === id);
            const res = sample ? sample.meta.resolution : null;

            const div = document.createElement('div');
            div.style.pointerEvents = 'auto';
            div.style.cursor = 'pointer';
            
            let html = masterLegends[id];
            if (res) {
                const resHtml = `<div class="legend-res">Res: ${res}m/px</div>`;
                html = html.replace('</div>', resHtml + '</div>');
            }
            
            div.innerHTML = html;
            div.onclick = () => {
                const parts = id.split('-');
                const sat = parts[0];
                const group = document.getElementById(`group-${sat}`);
                const prod = document.getElementById(`prod-${sat}-${parts.slice(1).join('-')}`);
                if (group) group.classList.remove('collapsed');
                if (prod) prod.classList.remove('collapsed');
                prod.scrollIntoView({ behavior: 'smooth', block: 'center' });
            };
            panel.appendChild(div);
        }
    });
}

// --- LAYER MANAGEMENT ---
async function toggleLayer(layerMeta, isVisible, element) {
    const path = layerMeta.path;
    const spinner = document.getElementById('map-spinner');

    if (isVisible) {
        // AUTO-ZOOM LOGIC: 
        // Only if no layers are currently visible AND new layer is outside current view
        const anyVisible = Object.values(activeLayers).some(obj => obj.layer.getVisible());
        if (!anyVisible) {
            const b = layerMeta.bounds;
            const layerExtent = ol.extent.boundingExtent([
                ol.proj.fromLonLat([b[0][1], b[0][0]]),
                ol.proj.fromLonLat([b[1][1], b[1][0]])
            ]);
            const viewExtent = map.getView().calculateExtent(map.getSize());
            if (!ol.extent.intersects(layerExtent, viewExtent)) {
                map.getView().fit(layerExtent, { padding: [50, 50, 50, 50], duration: 1000 });
            }
        }

        // If layer already exists, just make it visible
        if (activeLayers[path]) {
            activeLayers[path].layer.setVisible(true);
            element.classList.add('active');
            updateLegends();
            updateGroupMarkers();
            return;
        }

        element.classList.add('active', 'loading');
        spinner.style.display = 'block';
        
        try {
            // Safer URL construction
            const baseUrl = window.location.href.split('index.html')[0].split('?')[0];
            const url = baseUrl + IMAGE_BASE_URL + path;
            
            console.log("Loading COG:", url);

            // OPENLAYERS NATIVE COG SOURCE
            const source = new ol.source.GeoTIFF({
                sources: [{ url: url }],
                // OpenLayers WebGLTile often prefers to handle nodata internally 
                // but we can help it if needed.
                normalize: true,
                transition: 0 // Disable fade-in effect
            });

            const leafletLayer = new ol.layer.WebGLTile({
                source: source,
                opacity: 1,
                visible: true
            });

            // Set Z-index based on acquisition time
            const timestamp = new Date(layerMeta.acquisition_time).getTime();
            leafletLayer.setZIndex(Math.floor(timestamp / 100000));

            activeLayers[path] = { layer: leafletLayer, meta: layerMeta };
            map.addLayer(leafletLayer);
            updateLegends();
            updateGroupMarkers();

            // Spinner off once it starts loading tiles
            element.classList.remove('loading');
            spinner.style.display = 'none';

        } catch (err) {
            console.error("OL Load Error:", err);
            element.classList.remove('active', 'loading');
            element.querySelector('input').checked = false;
            updateGroupMarkers();
            spinner.style.display = 'none';
        }
    } else {
        element.classList.remove('active', 'loading');
        if (activeLayers[path]) {
            // Just hide it, don't remove it from the map or state
            activeLayers[path].layer.setVisible(false);
            updateLegends();
            updateGroupMarkers();
        }
    }
}
