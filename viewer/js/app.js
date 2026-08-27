/**
 * Satelliittikuvat Web Viewer (OpenLayers Edition)
 * High-performance native COG rendering with hardware acceleration.
 */

// --- CONFIGURATION ---
const IMAGE_BASE_URL = "imagery/"; 
const INVENTORY_URL = IMAGE_BASE_URL + "visual/inventory.json";
const LEGENDS_URL = IMAGE_BASE_URL + "legends/legends.json";
const CONFIG_URL = "config.json";

// --- LANGUAGE ---
let currentLang = localStorage.getItem('viewer_lang') || 'fi';

function setLanguage(lang) {
    if (!UI_TRANSLATIONS[lang]) return;
    currentLang = lang;
    localStorage.setItem('viewer_lang', lang);
    
    // Update UI
    translateUI();
    initBasePicker(); // Re-init base picker to update labels
    if (inventoryData.length > 0) {
        renderLayerPicker(inventoryData);
        updateAcquisitionRange(inventoryData);
    }
    
    // Update active lang button
    document.querySelectorAll('.lang-btn').forEach(btn => {
        btn.classList.toggle('active', btn.id === `lang-${lang}`);
    });
}

function translateUI() {
    const t = UI_TRANSLATIONS[currentLang];
    document.querySelectorAll('[data-i18n]').forEach(el => {
        const key = el.getAttribute('data-i18n');
        if (t[key]) el.innerText = t[key];
    });
    document.querySelectorAll('[data-i18n-title]').forEach(el => {
        const key = el.getAttribute('data-i18n-title');
        if (t[key]) el.title = t[key];
    });
}

// --- LAYER ORDERING (Z-Indices) ---
// Stacking order (bottom to top):
//   Main products & ROIs: time-of-flight (newer on top)
//   Identify Tiles (optical/radar grids)
//   Prediction GeoJSONs (overpass layers)
//   User GeoJSONs (config overlays)
const Z_INDEX_IDENTIFY = 100000000;
const Z_INDEX_HIGHLIGHT = 110000000;
const Z_INDEX_PREDICTIONS = 150000000;
const Z_INDEX_OVERLAYS = 200000000;

const S2_PRIORITY = ["TCI", "TCI-GF", "TCI-AIS", "NIRFC", "NIRFC-GF", "AP", "AP-GF", "NDBI_CLEAN", "NDBI", "NDRE", "NDVI", "NBR", "CAMO"];
const S1_PRIORITY = ["VV", "VH", "RATIO", "RATIO-AIS"];
const S3_PRIORITY = ["FIRE", "BT"];

// --- HELPERS ---
function formatSize(bytes) {
    if (!bytes || bytes === 0) return "0 B";
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + " " + sizes[i];
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
let roiSortMode = 'product'; // 'product' or 'roi'
let orbitFilter = { S1: 'all', S2: 'all', S3: 'all', FUSED: 'all', ROI: 'all' };
let identifyOpticalLayer;
let identifyRadarLayer;
let overpassS1Layer;
let overpassS2Layer;
let overpassS3Layer;
let masterLegends = {}; 
let sentinelAttribution = new ol.source.Vector({ attributions: '' });

// --- SETTINGS PERSISTENCE ---
const SETTINGS_KEY = 'sat_viewer_settings';

function saveSettings() {
    if (!map) return;
    const view = map.getView();
    const settings = {
        center: view.getCenter(),
        zoom: view.getZoom(),
        activeLayerPaths: Object.keys(activeLayers).filter(path => activeLayers[path].layer.getVisible()),
        s2SortMode: s2SortMode,
        roiSortMode: roiSortMode,
        orbitFilter: orbitFilter,
        expandedGroups: Array.from(document.querySelectorAll('.sat-group:not(.collapsed), .grid-group:not(.collapsed), .prod-group:not(.collapsed)'))
            .map(el => el.id).filter(id => !!id),
        identifyOptical: !!identifyOpticalLayer,
        identifyRadar: !!identifyRadarLayer,
        overpassS1: !!overpassS1Layer,
        overpassS2: !!overpassS2Layer,
        overpassS3: !!overpassS3Layer,
        fireOpacity: parseInt(document.getElementById('fire-opacity-slider').value) / 100,
        radarOpacity: parseInt(document.getElementById('radar-opacity-slider').value) / 100,
        baseLayer: Object.keys(baseLayers).find(key => baseLayers[key].getVisible()) || 'dark',
        sidebarCollapsed: document.body.classList.contains('sidebar-collapsed')
    };
    localStorage.setItem(SETTINGS_KEY, JSON.stringify(settings));
}

function loadSettings() {
    try {
        const saved = localStorage.getItem(SETTINGS_KEY);
        if (saved) {
            const parsed = JSON.parse(saved);
            if (parsed.sidebarCollapsed) {
                document.body.classList.add('sidebar-collapsed');
            }
            return parsed;
        }
    } catch (e) { return null; }
    return null;
}

const baseLayers = {
    'dark': new ol.layer.Tile({
        source: new ol.source.XYZ({
            url: 'https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}',
            attributions: 'Tiles &copy; Esri.',
            maxZoom: 16,
            crossOrigin: 'anonymous'
        }),
        visible: true
    }),
    'osm': new ol.layer.Tile({
        source: new ol.source.OSM({
            crossOrigin: 'anonymous'
        }),
        visible: false
    })
};

// --- INITIALIZATION ---
document.addEventListener('DOMContentLoaded', () => {
    setLanguage(currentLang);
    initMap();
    initBasePicker();
    loadConfig();
    loadInventory();
    loadLegends();
    checkLogo();

    document.getElementById('deselect-all').onclick = deselectAllLayers;
    document.getElementById('zoom-available').onclick = zoomToAvailable;
    document.getElementById('identify-optical').onclick = toggleIdentifyOptical;
    document.getElementById('identify-radar').onclick = toggleIdentifyRadar;
    document.getElementById('overpass-s1').onclick = toggleOverpassS1;
    document.getElementById('overpass-s2').onclick = toggleOverpassS2;
    document.getElementById('overpass-s3').onclick = toggleOverpassS3;
    document.getElementById('toggle-fullscreen').onclick = toggleFullscreen;
    document.getElementById('map-screenshot').onclick = takeScreenshot;
    document.getElementById('fire-opacity-slider').oninput = function() {
        const val = this.value / 100;
        document.getElementById('fire-opacity-value').innerText = Math.round(val * 100) + '%';
        Object.keys(activeLayers).forEach(path => {
            const o = activeLayers[path];
            if (o.meta.legend_id && o.meta.legend_id.startsWith('S3-FIRE')) {
                o.layer.setOpacity(val);
            }
        });
        saveSettings();
    };

    document.getElementById('radar-opacity-slider').oninput = function() {
        const val = this.value / 100;
        document.getElementById('radar-opacity-value').innerText = Math.round(val * 100) + '%';
        Object.keys(activeLayers).forEach(path => {
            const o = activeLayers[path];
            if (o.meta.legend_id && o.meta.legend_id.startsWith('S1-') && !o.meta.legend_id.startsWith('S1-AIS')) {
                o.layer.setOpacity(val);
            }
        });
        saveSettings();
    };

    document.getElementById('sidebar-toggle').onclick = () => {
        document.body.classList.toggle('sidebar-collapsed');
        saveSettings();
        setTimeout(() => map.updateSize(), 350); // Resize map after transition
    };
});

function deselectAllLayers() {
    const checkboxes = document.querySelectorAll('#layer-picker input[type="checkbox"]');
    checkboxes.forEach(chk => {
        if (chk.checked) {
            chk.checked = false;
            chk.dispatchEvent(new Event('change'));
        }
    });
}

function updateAcquisitionRange(layers) {
    const rangeEl = document.getElementById('acq-range');
    if (!rangeEl || !layers || layers.length === 0) return;
    const times = layers.map(l => l.acquisition_time).filter(t => t && t !== "Unknown").sort();
    if (times.length > 0) {
        const t = UI_TRANSLATIONS[currentLang];
        rangeEl.innerText = `${t.acq_range}: (${times[0]} - ${times[times.length - 1]})`;
    }
}

function updateGroupMarkers() {
    document.querySelectorAll('.sat-group, .grid-group, .prod-group').forEach(group => {
        group.classList.toggle('has-active', group.querySelectorAll('input:checked').length > 0);
    });
}

function updateBBoxWidget() {
    const el = document.getElementById('bbox-value');
    if (!el || !map) return;
    const extent = map.getView().calculateExtent(map.getSize());
    const bbox = ol.proj.transformExtent(extent, 'EPSG:3857', 'EPSG:4326');
    // Format: minLon,minLat,maxLon,maxLat with 4 decimal places
    el.innerText = bbox.map(v => v.toFixed(4)).join(',');
}

function initMap() {
    const saved = loadSettings();
    map = new ol.Map({
        target: 'map',
        controls: ol.control.defaults.defaults().extend([new ol.control.Attribution({ collapsible: false })]),
        layers: [baseLayers.dark, baseLayers.osm],
        view: new ol.View({
            center: saved ? saved.center : ol.proj.fromLonLat([24.9384, 60.1699]),
            zoom: saved ? saved.zoom : 8
        })
    });

    if (saved && saved.baseLayer) {
        Object.keys(baseLayers).forEach(key => baseLayers[key].setVisible(key === saved.baseLayer));
    }
    map.on('moveend', () => {
        saveSettings();
        updateLegends();
        updateBBoxWidget();
    });
    map.addLayer(new ol.layer.Vector({ source: sentinelAttribution }));
    map.addControl(new ol.control.ScaleLine({ units: 'metric' }));
    updateBBoxWidget();

    hoverSource = new ol.source.Vector();

    map.addLayer(new ol.layer.Vector({
        source: hoverSource, zIndex: Z_INDEX_HIGHLIGHT,
        style: new ol.style.Style({
            stroke: new ol.style.Stroke({ color: '#00bcd4', width: 3 }),
            fill: new ol.style.Fill({ color: 'rgba(0, 188, 212, 0.1)' })
        })
    }));

    highlightSource = new ol.source.Vector();
    map.addLayer(new ol.layer.Vector({
        source: highlightSource, zIndex: Z_INDEX_HIGHLIGHT,
        style: (f) => new ol.style.Style({
            stroke: new ol.style.Stroke({ color: '#ffeb3b', width: 3 }),
            fill: new ol.style.Fill({ color: 'rgba(255, 235, 59, 0.2)' }),
            text: new ol.style.Text({
                text: f.get('label'), font: f.get('isOptical') ? 'bold 14px sans-serif' : '11px sans-serif',
                fill: new ol.style.Fill({ color: '#ffeb3b' }), stroke: new ol.style.Stroke({ color: '#000', width: 3 })
            })
        })
    }));

    map.on('pointermove', (evt) => {
        if (evt.dragging) return;
        highlightSource.clear();
        const feature = map.forEachFeatureAtPixel(map.getEventPixel(evt.originalEvent), (f, l) => {
            if (l === identifyOpticalLayer || l === identifyRadarLayer) return f;
        });
        if (feature) {
            const isOpt = identifyOpticalLayer && identifyOpticalLayer.getSource().getFeatures().includes(feature);
            const clone = feature.clone();
            clone.set('isOptical', isOpt);
            highlightSource.addFeature(clone);
            map.getTargetElement().style.cursor = 'pointer';
        } else map.getTargetElement().style.cursor = '';
    });

    map.on('singleclick', (evt) => {
        const feature = map.forEachFeatureAtPixel(map.getEventPixel(evt.originalEvent), (f, l) => {
            if (l === identifyOpticalLayer || l === identifyRadarLayer) return f;
        });
        if (feature) {
            if (feature.get('isOptical')) jumpToSidebar('S2', 'TCI', feature.get('label'));
            else if (feature.get('isRadar')) jumpToSidebar('S1', 'RATIO', feature.get('time'));
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
        const target = prodGroup.querySelector(sat === 'S2' ? `.layer-item[data-grid="${identifier}"]` : `.layer-item[data-time="${identifier}"]`);
        if (target) {
            target.scrollIntoView({ behavior: 'smooth', block: 'center' });
            target.classList.add('jump-highlight');
            setTimeout(() => target.classList.remove('jump-highlight'), 2000);
        } else prodGroup.scrollIntoView({ behavior: 'smooth', block: 'center' });
    }
}

function initBasePicker() {
    const container = document.getElementById('base-picker');
    container.innerHTML = '';
    const t = UI_TRANSLATIONS[currentLang];
    ['dark', 'osm'].forEach(id => {
        const btn = document.createElement('button');
        btn.className = 'base-btn' + (baseLayers[id].getVisible() ? ' active' : '');
        btn.innerText = (id === 'dark' ? t.dark : t.map);
        btn.onclick = () => {
            Object.keys(baseLayers).forEach(key => baseLayers[key].setVisible(key === id));
            container.querySelectorAll('.base-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            saveSettings();
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
            if (config.overlays) loadOverlays(config.overlays);
        }
    } catch (e) {}
}

function loadOverlays(configs) {
    configs.forEach((cfg, index) => {
        const isObj = typeof cfg === 'object' && cfg !== null;
        const url = isObj ? cfg.url : cfg;
        const color = (isObj && cfg.color) ? cfg.color : '#ffeb3b';
        const width = (isObj && cfg.lineWidth) ? cfg.lineWidth : 2.5;
        const markerSize = (isObj && cfg.markerSize) ? cfg.markerSize : 6;
        let dash = null;
        if (isObj && cfg.lineStyle === 'dashed') dash = [10, 10];
        else if (isObj && cfg.lineStyle === 'dotted') dash = [2, 7];

        map.addLayer(new ol.layer.Vector({
            source: new ol.source.Vector({ url: url, format: new ol.format.GeoJSON() }),
            zIndex: Z_INDEX_OVERLAYS + index,
            style: (f) => {
                const type = f.getGeometry().getType();
                if (type.includes('Point')) {
                    return new ol.style.Style({
                        image: new ol.style.Circle({ radius: markerSize, fill: new ol.style.Fill({ color: color }), stroke: new ol.style.Stroke({ color: '#000', width: 2 }) }),
                        text: new ol.style.Text({ text: f.get('label') || f.get('name') || '', font: 'bold 13px sans-serif', fill: new ol.style.Fill({ color: '#fff' }), stroke: new ol.style.Stroke({ color: '#000', width: 3 }), offsetY: -(markerSize + 10), overflow: true })
                    });
                }
                return new ol.style.Style({ stroke: new ol.style.Stroke({ color: color, width: width, lineDash: dash }), fill: new ol.style.Fill({ color: 'rgba(0,0,0,0)' }) });
            }
        }));
    });
}

async function loadLegends() {
    try {
        const resp = await fetch(window.location.origin + window.location.pathname.replace('index.html', '') + LEGENDS_URL);
        if (resp.ok) masterLegends = await resp.json();
    } catch (e) {}
}

async function loadInventory() {
    const picker = document.getElementById('layer-picker');
    const t = UI_TRANSLATIONS[currentLang];
    const saved = loadSettings();
    if (saved && saved.s2SortMode) s2SortMode = saved.s2SortMode;
    if (saved && saved.roiSortMode) roiSortMode = saved.roiSortMode;
    if (saved && saved.orbitFilter) orbitFilter = saved.orbitFilter;

    try {
        const response = await fetch(INVENTORY_URL);
        if (!response.ok) throw new Error(response.status);
        const data = await response.json();

        if (data.layers && data.layers.length > 0) {
            inventoryData = data.layers;
            updateAcquisitionRange(data.layers);
            renderLayerPicker(data.layers);
            
            if (saved && saved.activeLayerPaths) {
                saved.activeLayerPaths.forEach(path => {
                    const meta = inventoryData.find(l => l.path === path);
                    const chk = document.getElementById(`chk-${path.replace(/[^a-zA-Z0-9]/g, '_')}`);
                    if (meta && chk) {
                        chk.checked = true;
                        toggleLayer(meta, true, chk.closest('.layer-item'), true);
                    }
                });
            }
            if (saved) {
                if (saved.identifyOptical) toggleIdentifyOptical();
                if (saved.identifyRadar) toggleIdentifyRadar();
                if (saved.overpassS1) toggleOverpassS1();
                if (saved.overpassS2) toggleOverpassS2();
                if (saved.overpassS3) toggleOverpassS3();
                if (saved.fireOpacity != null) {
                    const pct = Math.round(saved.fireOpacity * 100);
                    document.getElementById('fire-opacity-slider').value = pct;
                    document.getElementById('fire-opacity-value').innerText = pct + '%';
                }
                if (saved.radarOpacity != null) {
                    const pct = Math.round(saved.radarOpacity * 100);
                    document.getElementById('radar-opacity-slider').value = pct;
                    document.getElementById('radar-opacity-value').innerText = pct + '%';
                }
            }
        } else {
            picker.innerHTML = `<div id="loading">${t.no_images}</div>`;
        }
    } catch (e) { picker.innerHTML = `<div id="loading">${t.error_loading}</div>`; }
}

function zoomToAvailable() {
    let extent = ol.extent.createEmpty();
    inventoryData.forEach(l => {
        if (!l.bounds) return;
        ol.extent.extend(extent, ol.extent.boundingExtent([ol.proj.fromLonLat([l.bounds[0][1], l.bounds[0][0]]), ol.proj.fromLonLat([l.bounds[1][1], l.bounds[1][0]])]));
    });
    if (!ol.extent.isEmpty(extent)) map.getView().fit(extent, { padding: [50, 50, 50, 50], duration: 1000 });
}

function toggleFullscreen() {
    if (!document.fullscreenElement) {
        document.documentElement.requestFullscreen().catch(err => {
            console.error(`Error attempting to enable full-screen mode: ${err.message}`);
        });
    } else {
        document.exitFullscreen();
    }
}

function takeScreenshot() {
    document.getElementById('map-spinner').style.display = 'block';
    
    // Perform a synchronous render to ensure canvases are up to date
    map.renderSync();
    
    // Execute immediately after renderSync for maximum speed
    const mapCanvas = document.createElement('canvas');
    const size = map.getSize();
    mapCanvas.width = size[0];
    mapCanvas.height = size[1];
    const mapContext = mapCanvas.getContext('2d');
    
    // 1. Draw Map Layers
    const layers = document.querySelectorAll('.ol-layer canvas, canvas.ol-layer');
    layers.forEach(canvas => {
        if (canvas.width > 0) {
            const opacity = canvas.parentNode.style.opacity || canvas.style.opacity;
            mapContext.globalAlpha = opacity === '' ? 1 : Number(opacity);
            
            const style = window.getComputedStyle(canvas);
            const transform = style.getPropertyValue('transform');
            if (transform !== 'none') {
                const matrix = transform.match(/^matrix\(([^\(]*)\)$/)[1].split(',').map(Number);
                CanvasRenderingContext2D.prototype.setTransform.apply(mapContext, matrix);
            }
            mapContext.drawImage(canvas, 0, 0);
            mapContext.setTransform(1, 0, 0, 1, 0, 0);
        }
    });
    
    mapContext.globalAlpha = 1;

    // 2. Draw Logo
    const logoCont = document.getElementById('logo-container');
    if (logoCont && window.getComputedStyle(logoCont).display !== 'none') {
        const logoImg = logoCont.querySelector('img');
        if (logoImg && logoImg.complete) {
            mapContext.drawImage(logoImg, 20, 15, logoImg.width, logoImg.height);
        }
    }

    // 3. Draw Legend Panel
    const legendPanel = document.getElementById('legend-panel');
    if (legendPanel && legendPanel.children.length > 0) {
        const mapRect = document.getElementById('map').getBoundingClientRect();
        const panelRect = legendPanel.getBoundingClientRect();
        
        const bgX = panelRect.left - mapRect.left;
        const bgY = panelRect.top - mapRect.top;
        const bgW = panelRect.width;
        const bgH = panelRect.height;

        mapContext.fillStyle = 'rgba(26, 26, 26, 0.9)';
        mapContext.fillRect(bgX, bgY, bgW, bgH);
        
        const allElements = legendPanel.querySelectorAll('*');
        allElements.forEach(el => {
            const rect = el.getBoundingClientRect();
            const x = rect.left - mapRect.left;
            const y = rect.top - mapRect.top;
            const style = window.getComputedStyle(el);
            const width = rect.width;
            const height = rect.height;

            // A. Draw Borders
            const bTop = parseFloat(style.borderTopWidth);
            if (bTop > 0 && style.borderTopStyle !== 'none') {
                mapContext.strokeStyle = style.borderTopColor;
                mapContext.lineWidth = bTop;
                mapContext.beginPath();
                mapContext.moveTo(x, y);
                mapContext.lineTo(x + width, y);
                mapContext.stroke();
            }

            // B. Draw Backgrounds
            const bg = style.backgroundColor;
            const grad = style.backgroundImage;

            if (height > 0 && width > 0) {
                let hasDrawnBg = false;
                if (grad && grad.includes('linear-gradient')) {
                    const colors = grad.match(/rgb\([^)]+\)|rgba\([^)]+\)|#[a-fA-F0-9]{3,6}/g);
                    if (colors) {
                        const canvasGrad = mapContext.createLinearGradient(x, 0, x + width, 0);
                        colors.forEach((color, idx) => canvasGrad.addColorStop(idx / (colors.length - 1), color));
                        mapContext.fillStyle = canvasGrad;
                        mapContext.fillRect(x, y, width, height);
                        hasDrawnBg = true;
                    }
                } else if (bg && bg !== 'rgba(0, 0, 0, 0)' && bg !== 'transparent' && bg !== 'rgb(26, 26, 26)') {
                    mapContext.fillStyle = bg;
                    mapContext.fillRect(x, y, width, height);
                    hasDrawnBg = true;
                }
                
                if (hasDrawnBg && (width > 50 || el.style.border)) {
                    mapContext.strokeStyle = '#444';
                    mapContext.lineWidth = 1;
                    mapContext.strokeRect(x, y, width, height);
                }
            }

            // C. Draw Text with wrapping and padding support
            let nodeText = "";
            for (let i = 0; i < el.childNodes.length; i++) {
                if (el.childNodes[i].nodeType === 3) {
                    nodeText += el.childNodes[i].nodeValue.trim();
                }
            }

            if (nodeText) {
                mapContext.fillStyle = style.color || '#fff';
                const weight = style.fontWeight === 'bold' || parseInt(style.fontWeight) >= 600 ? 'bold' : 'normal';
                const size = style.fontSize || '10px';
                const font = style.fontFamily || 'sans-serif';
                mapContext.font = `${weight} ${size} ${font}`;
                
                const pTop = parseFloat(style.paddingTop) || 0;
                const pLeft = parseFloat(style.paddingLeft) || 0;
                
                const lineHeight = parseFloat(size) * 1.2;
                const maxWidth = panelRect.width - (rect.left - panelRect.left) - (parseFloat(style.paddingRight) || 0);
                
                let currentY = y + pTop + parseFloat(size) * 0.8;
                const words = nodeText.split(' ');
                let line = '';

                for (let n = 0; n < words.length; n++) {
                    const testLine = line + words[n] + ' ';
                    const metrics = mapContext.measureText(testLine);
                    if (metrics.width > maxWidth && n > 0) {
                        mapContext.fillText(line, x + pLeft, currentY);
                        line = words[n] + ' ';
                        currentY += lineHeight;
                    } else {
                        line = testLine;
                    }
                }
                mapContext.fillText(line, x + pLeft, currentY);
            }
        });
    }

    // 4. Draw Scale Line
    const scaleLine = document.querySelector('.ol-scale-line-inner');
    if (scaleLine) {
        const text = scaleLine.innerText;
        const width = scaleLine.offsetWidth;
        const x = 330; // Matches CSS left: 330px
        const y = size[1] - 25;

        
        mapContext.fillStyle = 'rgba(0, 0, 0, 0.8)';
        mapContext.fillRect(x - 5, y - 15, width + 10, 20);
        mapContext.strokeStyle = '#ffeb3b';
        mapContext.lineWidth = 2;
        mapContext.beginPath();
        mapContext.moveTo(x, y);
        mapContext.lineTo(x, y + 5);
        mapContext.lineTo(x + width, y + 5);
        mapContext.lineTo(x + width, y);
        mapContext.stroke();
        
        mapContext.fillStyle = '#ffeb3b';
        mapContext.font = 'bold 11px sans-serif';
        mapContext.fillText(text, x + 5, y - 2);
    }

    // 5. Draw Attributions
    const attrSet = new Set();
    document.querySelectorAll('.ol-attribution ul li').forEach(li => {
        let text = li.innerText.trim();
        text = text.replace(' Tiles © Esri.', '© Esri');
        if (text) attrSet.add(text);
    });
    const sentAttr = sentinelAttribution.getAttributions()({}).join(' ');
    if (sentAttr) attrSet.add(sentAttr);
    
    const attrText = Array.from(attrSet).join(' | ');
    if (attrText) {
        mapContext.font = '10px monospace';
        const textWidth = mapContext.measureText(attrText).width;
        mapContext.fillStyle = 'rgba(0, 0, 0, 0.8)';
        mapContext.fillRect(size[0] - textWidth - 25, size[1] - 25, textWidth + 15, 18);
        mapContext.fillStyle = '#ffeb3b';
        mapContext.fillText(attrText, size[0] - textWidth - 17, size[1] - 13);
    }

    const link = document.createElement('a');
    link.download = `sat-screenshot-${new Date().toISOString().replace(/[:.]/g, '-')}.png`;
    link.href = mapCanvas.toDataURL();
    link.click();
    document.getElementById('map-spinner').style.display = 'none';
}

function toggleIdentifyOptical() {
    const btn = document.getElementById('identify-optical');
    if (identifyOpticalLayer) {
        map.removeLayer(identifyOpticalLayer); identifyOpticalLayer = null; highlightSource.clear();
        btn.classList.remove('active'); saveSettings(); return;
    }
    const source = new ol.source.Vector();
    const grids = {};
    inventoryData.forEach(l => {
        if (!l.product.startsWith('S2')) return;
        const grid = getGridSquare(l);
        if (!grid || (!grids[grid] || l.product === 'S2-TCI')) grids[grid] = l;
    });
    Object.keys(grids).forEach(id => {
        const l = grids[id];
        const f = l.footprint ? (new ol.format.GeoJSON()).readFeature(l.footprint, { dataProjection: 'EPSG:4326', featureProjection: 'EPSG:3857' }) : new ol.Feature(new ol.geom.Polygon([[ol.proj.fromLonLat([l.bounds[0][1], l.bounds[0][0]]), ol.proj.fromLonLat([l.bounds[1][1], l.bounds[0][0]]), ol.proj.fromLonLat([l.bounds[1][1], l.bounds[1][0]]), ol.proj.fromLonLat([l.bounds[0][1], l.bounds[1][0]]), ol.proj.fromLonLat([l.bounds[0][1], l.bounds[0][0]])]]));
        f.set('label', id); f.set('isOptical', true); source.addFeature(f);
    });
    identifyOpticalLayer = new ol.layer.Vector({ source: source, zIndex: Z_INDEX_IDENTIFY, style: (f) => new ol.style.Style({ stroke: new ol.style.Stroke({ color: '#3f51b5', width: 2 }), fill: new ol.style.Fill({ color: 'rgba(63, 81, 181, 0.05)' }), text: new ol.style.Text({ text: f.get('label'), font: 'bold 14px sans-serif', fill: new ol.style.Fill({ color: '#3f51b5' }), stroke: new ol.style.Stroke({ color: '#fff', width: 2 }) }) }) });
    map.addLayer(identifyOpticalLayer); btn.classList.add('active'); saveSettings();
}

function toggleIdentifyRadar() {
    const btn = document.getElementById('identify-radar');
    if (identifyRadarLayer) {
        map.removeLayer(identifyRadarLayer); identifyRadarLayer = null; highlightSource.clear();
        btn.classList.remove('active'); saveSettings(); return;
    }
    const source = new ol.source.Vector(), seen = new Set();
    inventoryData.forEach(l => {
        if (!l.product.startsWith('S1') || seen.has(l.acquisition_time)) return;
        seen.add(l.acquisition_time);
        const f = l.footprint ? (new ol.format.GeoJSON()).readFeature(l.footprint, { dataProjection: 'EPSG:4326', featureProjection: 'EPSG:3857' }) : new ol.Feature(new ol.geom.Polygon([[ol.proj.fromLonLat([l.bounds[0][1], l.bounds[0][0]]), ol.proj.fromLonLat([l.bounds[1][1], l.bounds[0][0]]), ol.proj.fromLonLat([l.bounds[1][1], l.bounds[1][0]]), ol.proj.fromLonLat([l.bounds[0][1], l.bounds[1][0]]), ol.proj.fromLonLat([l.bounds[0][1], l.bounds[0][0]])]]));
        const date = new Date(l.acquisition_time);
        f.set('label', date.toLocaleString(currentLang === 'fi' ? 'fi-FI' : (currentLang === 'sv' ? 'sv-SE' : (currentLang === 'de' ? 'de-DE' : 'en-GB')), { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', timeZone: 'UTC' }) + "Z");
        f.set('time', l.acquisition_time); f.set('isRadar', true); source.addFeature(f);
    });
    identifyRadarLayer = new ol.layer.Vector({ source: source, zIndex: Z_INDEX_IDENTIFY + 1, style: (f) => new ol.style.Style({ stroke: new ol.style.Stroke({ color: '#3f51b5', width: 2 }), fill: new ol.style.Fill({ color: 'rgba(63, 81, 181, 0.05)' }), text: new ol.style.Text({ text: f.get('label'), font: '11px sans-serif', fill: new ol.style.Fill({ color: '#3f51b5' }), stroke: new ol.style.Stroke({ color: '#fff', width: 2 }), overflow: true }) }) });
    map.addLayer(identifyRadarLayer); btn.classList.add('active'); saveSettings();
}

function toggleOverpassS1() {
    const btn = document.getElementById('overpass-s1');
    if (overpassS1Layer) {
        map.removeLayer(overpassS1Layer);
        overpassS1Layer = null;
        btn.classList.remove('active');
        saveSettings();
        return;
    }
    const url = IMAGE_BASE_URL + 'visual/overpass_s1.geojson';
    overpassS1Layer = new ol.layer.Vector({
        source: new ol.source.Vector({ url: url, format: new ol.format.GeoJSON() }),
        zIndex: Z_INDEX_PREDICTIONS,
        style: (f) => {
            const ts = f.get('timestamp');
            return new ol.style.Style({
                stroke: new ol.style.Stroke({ color: '#ffeb3b', width: 2 }),
                fill: new ol.style.Fill({ color: 'rgba(255, 235, 59, 0.08)' }),
                text: ts ? new ol.style.Text({
                    text: ts, font: '11px monospace',
                    fill: new ol.style.Fill({ color: '#ffeb3b' }),
                    stroke: new ol.style.Stroke({ color: '#000', width: 3 }),
                    overflow: true
                }) : undefined
            });
        }
    });
    map.addLayer(overpassS1Layer);
    btn.classList.add('active');
    saveSettings();
}

function toggleOverpassS2() {
    const btn = document.getElementById('overpass-s2');
    if (overpassS2Layer) {
        map.removeLayer(overpassS2Layer);
        overpassS2Layer = null;
        btn.classList.remove('active');
        saveSettings();
        return;
    }
    const url = IMAGE_BASE_URL + 'visual/overpass_s2.geojson';
    overpassS2Layer = new ol.layer.Vector({
        source: new ol.source.Vector({ url: url, format: new ol.format.GeoJSON() }),
        zIndex: Z_INDEX_PREDICTIONS + 1,
        style: (f) => {
            const ts = f.get('timestamp');
            return new ol.style.Style({
                stroke: new ol.style.Stroke({ color: '#00bcd4', width: 2 }),
                fill: new ol.style.Fill({ color: 'rgba(0, 188, 212, 0.08)' }),
                text: ts ? new ol.style.Text({
                    text: ts, font: '11px monospace',
                    fill: new ol.style.Fill({ color: '#00bcd4' }),
                    stroke: new ol.style.Stroke({ color: '#000', width: 3 }),
                    overflow: true
                }) : undefined
            });
        }
    });
    map.addLayer(overpassS2Layer);
    btn.classList.add('active');
    saveSettings();
}

function toggleOverpassS3() {
    const btn = document.getElementById('overpass-s3');
    if (overpassS3Layer) {
        map.removeLayer(overpassS3Layer);
        overpassS3Layer = null;
        btn.classList.remove('active');
        saveSettings();
        return;
    }
    const url = IMAGE_BASE_URL + 'visual/overpass_s3.geojson';
    overpassS3Layer = new ol.layer.Vector({
        source: new ol.source.Vector({ url: url, format: new ol.format.GeoJSON() }),
        zIndex: Z_INDEX_PREDICTIONS + 2,
        style: (f) => {
            const ts = f.get('timestamp');
            return new ol.style.Style({
                stroke: new ol.style.Stroke({ color: '#ff4444', width: 2 }),
                fill: new ol.style.Fill({ color: 'rgba(255, 68, 68, 0.08)' }),
                text: ts ? new ol.style.Text({
                    text: ts, font: '11px monospace',
                    fill: new ol.style.Fill({ color: '#ff4444' }),
                    stroke: new ol.style.Stroke({ color: '#000', width: 3 }),
                    overflow: true
                }) : undefined
            });
        }
    });
    map.addLayer(overpassS3Layer);
    btn.classList.add('active');
    saveSettings();
}

function getGridSquare(l) {
    if (!l.product.startsWith("S2")) return "";
    const fn = l.path.split('/').pop();
    return fn.startsWith('T') ? fn.split('-')[0] : "";
}

function getRoiName(l) {
    if (!l.product.startsWith("ROI")) return "";
    const fn = l.path.split('/').pop();
    // Format: ROI-Name-Prod-Time.tif or Name_Prod_Time.tif
    // The current roi_manager uses Name_Prod_Time.tif
    return fn.split('_')[0];
}

function setS2SortMode(mode) {
    if (s2SortMode === mode) return;
    s2SortMode = mode; saveSettings(); renderLayerPicker(inventoryData);
}

function setRoiSortMode(mode) {
    if (roiSortMode === mode) return;
    roiSortMode = mode; saveSettings(); renderLayerPicker(inventoryData);
}

function setOrbitFilter(sat, mode) {
    if (orbitFilter[sat] === mode) return;
    orbitFilter[sat] = mode; saveSettings(); renderLayerPicker(inventoryData);
}

function matchesOrbitFilter(l, sat) {
    const filter = orbitFilter[sat] || 'all';
    if (filter === 'all') return true;
    return (l.orbit_direction || '').toUpperCase() === filter;
}

function renderLayerPicker(layers) {
    const picker = document.getElementById('layer-picker');
    const pt = PRODUCT_TRANSLATIONS[currentLang];
    const saved = loadSettings();
    const expandedIds = new Set(saved ? saved.expandedGroups : []);
    
    if (!saved) {
        ['S2', 'S1', 'S3', 'FUSED', 'ROI'].forEach(sat => {
            const old = document.getElementById(`group-${sat}`);
            if (old && !old.classList.contains('collapsed')) expandedIds.add(`group-${sat}`);
        });
    }

    picker.innerHTML = ''; 
    const groups = {};
    layers.forEach(l => {
        let sat, type;
        if (l.product.startsWith('ROI-')) {
            sat = 'ROI';
            // product is ROI-Name-ProductType
            const parts = l.product.split('-');
            
            const lastPart = parts[parts.length - 1];
            const lastTwo = parts.slice(-2).join('-');
            
            // Use the base product type for grouping (TCI, NDBI_CLEAN, etc.)
            if (PRODUCT_TRANSLATIONS[currentLang][lastTwo]) {
                type = lastTwo;
            } else {
                type = lastPart;
            }
        } else {
            const parts = l.product.split('-');
            sat = parts[0];
            type = parts.slice(1).join('-');
        }
        if (!groups[sat]) groups[sat] = {};
        if (!groups[sat][type]) groups[sat][type] = [];
        groups[sat][type].push(l);
    });

    ['S2', 'S1', 'S3', 'FUSED', 'ROI'].forEach(sat => {
        if (!groups[sat]) return;
        const satMeta = pt[sat] || { title: sat, subtitle: "" };
        const satDiv = document.createElement('div');
        satDiv.className = 'sat-group' + (expandedIds.has(`group-${sat}`) ? '' : ' collapsed');
        satDiv.id = `group-${sat}`;
        
        const f = orbitFilter[sat] || 'all';
        let sortRow = '';
        if (sat === 'S2') {
            sortRow = `<div class="sort-row"><button class="sort-btn ${s2SortMode === 'product' ? 'active' : ''}" onclick="event.stopPropagation(); setS2SortMode('product')">${UI_TRANSLATIONS[currentLang].by_product}</button><button class="sort-btn ${s2SortMode === 'grid' ? 'active' : ''}" onclick="event.stopPropagation(); setS2SortMode('grid')">${UI_TRANSLATIONS[currentLang].by_grid}</button></div>`;
        } else if (sat === 'ROI') {
            sortRow = `<div class="sort-row"><button class="sort-btn ${roiSortMode === 'product' ? 'active' : ''}" onclick="event.stopPropagation(); setRoiSortMode('product')">${UI_TRANSLATIONS[currentLang].by_product}</button><button class="sort-btn ${roiSortMode === 'roi' ? 'active' : ''}" onclick="event.stopPropagation(); setRoiSortMode('roi')">${UI_TRANSLATIONS[currentLang].by_roi}</button></div>`;
        }
        sortRow += `<div class="orbit-row"><button class="orbit-btn ${f === 'all' ? 'active' : ''}" onclick="event.stopPropagation(); setOrbitFilter('${sat}', 'all')" title="All">A</button><button class="orbit-btn ${f === 'ASCENDING' ? 'active' : ''}" onclick="event.stopPropagation(); setOrbitFilter('${sat}', 'ASCENDING')" title="Ascending">&uarr;</button><button class="orbit-btn ${f === 'DESCENDING' ? 'active' : ''}" onclick="event.stopPropagation(); setOrbitFilter('${sat}', 'DESCENDING')" title="Descending">&darr;</button></div>`;

        satDiv.innerHTML = `
            <div class="sat-title" onclick="this.parentElement.classList.toggle('collapsed'); saveSettings();">
                <span>${satMeta.title} <small>${satMeta.subtitle}</small></span>
            </div>
            ${sortRow}
            <div class="prod-container"></div>
        `;
        const prodContainer = satDiv.querySelector('.prod-container');

        if (sat === 'S2' && s2SortMode === 'grid') {
            const gridGroups = {};
            Object.keys(groups[sat]).forEach(type => {
                groups[sat][type].forEach(l => {
                    const grid = getGridSquare(l) || UI_TRANSLATIONS[currentLang].unknown;
                    if (!gridGroups[grid]) gridGroups[grid] = {};
                    if (!gridGroups[grid][type]) gridGroups[grid][type] = [];
                    gridGroups[grid][type].push(l);
                });
            });

            Object.keys(gridGroups).sort().forEach(grid => {
                const gridDiv = document.createElement('div');
                const gid = `grid-S2-${grid}`;
                gridDiv.className = 'grid-group' + (expandedIds.has(gid) ? '' : ' collapsed');
                gridDiv.id = gid;
                gridDiv.innerHTML = `<div class="grid-title" onclick="this.parentElement.classList.toggle('collapsed'); saveSettings();">${grid}</div><div class="prod-container"></div>`;
                gridDiv.querySelector('.grid-title').onmouseenter = () => {
                    const rep = gridGroups[grid]['TCI'] ? gridGroups[grid]['TCI'][0] : gridGroups[grid][Object.keys(gridGroups[grid])[0]][0];
                    if (rep) showLayerHover(rep);
                };
                gridDiv.querySelector('.grid-title').onmouseleave = () => hoverSource.clear();
                const gpc = gridDiv.querySelector('.prod-container');
                Object.keys(gridGroups[grid]).sort((a,b) => (S2_PRIORITY.indexOf(a) - S2_PRIORITY.indexOf(b)) || a.localeCompare(b)).forEach(type => {
                    const typeDiv = document.createElement('div');
                    const tid = `prod-S2-${grid}-${type}`;
                    typeDiv.className = 'prod-group' + (expandedIds.has(tid) ? '' : ' collapsed');
                    typeDiv.id = tid;
                    typeDiv.innerHTML = `<div class="prod-title" onclick="this.parentElement.classList.toggle('collapsed'); saveSettings();">${pt[type] ? pt[type].title : type}</div><div class="layer-container"></div>`;
                    const lc = typeDiv.querySelector('.layer-container');
                    gridGroups[grid][type].sort((a,b) => b.acquisition_time.localeCompare(a.acquisition_time)).filter(l => matchesOrbitFilter(l, sat)).forEach(l => lc.appendChild(createLayerItem(l)));
                    gpc.appendChild(typeDiv);
                });
                prodContainer.appendChild(gridDiv);
            });
        } else if (sat === 'ROI' && roiSortMode === 'roi') {
            const gridGroups = {};
            Object.keys(groups[sat]).forEach(type => {
                groups[sat][type].forEach(l => {
                    const roiName = getRoiName(l) || UI_TRANSLATIONS[currentLang].unknown;
                    if (!gridGroups[roiName]) gridGroups[roiName] = {};
                    if (!gridGroups[roiName][type]) gridGroups[roiName][type] = [];
                    gridGroups[roiName][type].push(l);
                });
            });

            Object.keys(gridGroups).sort().forEach(roiName => {
                const gridDiv = document.createElement('div');
                const gid = `grid-ROI-${roiName}`;
                gridDiv.className = 'grid-group' + (expandedIds.has(gid) ? '' : ' collapsed');
                gridDiv.id = gid;
                gridDiv.innerHTML = `<div class="grid-title" onclick="this.parentElement.classList.toggle('collapsed'); saveSettings();">${roiName}</div><div class="prod-container"></div>`;
                gridDiv.querySelector('.grid-title').onmouseenter = () => {
                    const rep = gridGroups[roiName]['ROI-TCI'] ? gridGroups[roiName]['ROI-TCI'][0] : gridGroups[roiName][Object.keys(gridGroups[roiName])[0]][0];
                    if (rep) showLayerHover(rep);
                };
                gridDiv.querySelector('.grid-title').onmouseleave = () => hoverSource.clear();
                const gpc = gridDiv.querySelector('.prod-container');
                Object.keys(gridGroups[roiName]).sort().forEach(type => {
                    const typeDiv = document.createElement('div');
                    const tid = `prod-ROI-${roiName}-${type}`;
                    typeDiv.className = 'prod-group' + (expandedIds.has(tid) ? '' : ' collapsed');
                    typeDiv.id = tid;
                    typeDiv.innerHTML = `<div class="prod-title" onclick="this.parentElement.classList.toggle('collapsed'); saveSettings();">${pt[type] ? pt[type].title : type}</div><div class="layer-container"></div>`;
                    const lc = typeDiv.querySelector('.layer-container');
                    gridGroups[roiName][type].sort((a,b) => b.acquisition_time.localeCompare(a.acquisition_time)).filter(l => matchesOrbitFilter(l, sat)).forEach(l => lc.appendChild(createLayerItem(l)));
                    gpc.appendChild(typeDiv);
                });
                prodContainer.appendChild(gridDiv);
            });
        } else {
            Object.keys(groups[sat]).sort((a,b) => {
                let prio = S2_PRIORITY;
                if (sat === 'S1') prio = S1_PRIORITY;
                if (sat === 'S3') prio = S3_PRIORITY;
                const idxA = prio.indexOf(a), idxB = prio.indexOf(b);
                if (idxA !== -1 && idxB !== -1) return idxA - idxB;
                if (idxA !== -1) return -1; if (idxB !== -1) return 1;
                return a.localeCompare(b);
            }).forEach(type => {
                const typeDiv = document.createElement('div');
                const tid = `prod-${sat}-${type}`;
                typeDiv.className = 'prod-group' + (expandedIds.has(tid) ? '' : ' collapsed');
                typeDiv.id = tid;
                typeDiv.innerHTML = `<div class="prod-title" onclick="this.parentElement.classList.toggle('collapsed'); saveSettings();">${pt[type] ? pt[type].title : type} <span class="subtitle">${pt[type] ? pt[type].subtitle : ''}</span></div><div class="layer-container"></div>`;
                const lc = typeDiv.querySelector('.layer-container');
                groups[sat][type].sort((a,b) => {
                    const timeComp = b.acquisition_time.localeCompare(a.acquisition_time);
                    if (sat === "S2" && timeComp === 0) {
                        return getGridSquare(a).localeCompare(getGridSquare(b));
                    }
                    return timeComp;
                }).filter(l => matchesOrbitFilter(l, sat)).forEach(l => lc.appendChild(createLayerItem(l)));
                prodContainer.appendChild(typeDiv);
            });
        }
        picker.appendChild(satDiv);
    });
    updateGroupMarkers();
}

function showLayerHover(l) {
    const f = l.footprint ? (new ol.format.GeoJSON()).readFeature(l.footprint, { dataProjection: 'EPSG:4326', featureProjection: 'EPSG:3857' }) : new ol.Feature(new ol.geom.Polygon([[ol.proj.fromLonLat([l.bounds[0][1], l.bounds[0][0]]), ol.proj.fromLonLat([l.bounds[1][1], l.bounds[0][0]]), ol.proj.fromLonLat([l.bounds[1][1], l.bounds[1][0]]), ol.proj.fromLonLat([l.bounds[0][1], l.bounds[1][0]]), ol.proj.fromLonLat([l.bounds[0][1], l.bounds[0][0]])]]));
    hoverSource.addFeature(f);
}

function createLayerItem(l) {
    const div = document.createElement('div');
    const isActive = activeLayers[l.path] && activeLayers[l.path].layer.getVisible();
    div.className = 'layer-item' + (isActive ? ' active' : '');
    const grid = getGridSquare(l); if (grid) div.dataset.grid = grid;
    const roiName = getRoiName(l); if (roiName) div.dataset.roi = roiName;
    div.dataset.time = l.acquisition_time;
    const t = UI_TRANSLATIONS[currentLang];
    const friendlyTime = (new Date(l.acquisition_time)).toLocaleString(currentLang === 'fi' ? 'fi-FI' : (currentLang === 'sv' ? 'sv-SE' : (currentLang === 'de' ? 'de-DE' : 'en-GB')), { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", timeZone: "UTC" }) + "Z";
    const safeId = `chk-${l.path.replace(/[^a-zA-Z0-9]/g, '_')}`;
    
    // Label starts with date/time
    let label = friendlyTime;
    if (grid) label += ` • ${grid}`;
    if (roiName) label += ` • ${roiName}`;

    // Info line (layer-status) reformatting
    const datePart = l.acquisition_time.split("T")[0];
    const sv = l.satellite ? l.satellite.substring(1) : "";
    let info = datePart;
    if (sv) info += ` • ${sv}`;
    if (l.cloud_cover != null) info += ` • ☁️ ${l.cloud_cover}%`;
    if (l.orbit_direction) {
        const arrow = l.orbit_direction.toUpperCase() === 'ASCENDING' ? '↑' : '↓';
        info += ` • ${arrow}`;
    }
    
    div.innerHTML = `<input type="checkbox" id="${safeId}" ${isActive ? 'checked' : ''}><div class="layer-info"><span class="layer-time">${label}</span><span class="layer-status">${info}</span></div><div class="layer-actions"><button class="dl-btn" title="${t.download_tif}"><svg viewBox="0 0 24 24" width="16" height="16"><path fill="currentColor" d="M12 16l-5-5h3V4h4v7h3l-5 5zm9 2v2H3v-2h18z"/></svg></button><span class="file-size">${formatSize(l.file_size_bytes)}</span></div>`;
    div.onclick = (e) => {
        if (e.target.closest('.dl-btn')) { e.stopPropagation(); downloadFile(window.location.href.split('index.html')[0].split('?')[0] + IMAGE_BASE_URL + l.path, l.path.split('/').pop()); return; }
        if (e.target.tagName !== 'INPUT') { const chk = div.querySelector('input'); chk.checked = !chk.checked; toggleLayer(l, chk.checked, div); }
    };
    div.querySelector('input').onchange = (e) => toggleLayer(l, e.target.checked, div);
    div.onmouseenter = () => { if (!div.querySelector('input').checked) showLayerHover(l); };
    div.onmouseleave = () => hoverSource.clear();
    return div;
}

function updateAttributions() {
    const years = new Set();
    Object.values(activeLayers).forEach(o => { if (o.layer.getVisible()) years.add(o.meta.acquisition_time.split('-')[0]); });
    if (years.size === 0) { sentinelAttribution.setAttributions([]); return; }
    const sorted = Array.from(years).sort();
    sentinelAttribution.setAttributions([`Made with Copernicus Sentinel Data ${sorted.length > 1 ? `${sorted[0]}-${sorted[sorted.length - 1]}` : sorted[0]}`]);
}

function updateLegends() {
    updateAttributions();
    const panel = document.getElementById('legend-panel'); panel.innerHTML = '';
    const activeIds = new Set();
    Object.values(activeLayers).forEach(o => { if (o.layer.getVisible() && o.meta.legend_id) activeIds.add(o.meta.legend_id); });
    activeIds.forEach(id => {
        if (masterLegends[id]) {
            const extent = map.getView().calculateExtent(map.getSize());
            const layersForLegend = Object.values(activeLayers).filter(o => {
                if (!o.layer.getVisible() || o.meta.legend_id !== id) return false;
                const b = o.meta.bounds;
                const layerExtent = ol.extent.boundingExtent([ol.proj.fromLonLat([b[0][1], b[0][0]]), ol.proj.fromLonLat([b[1][1], b[1][0]])]);
                return ol.extent.intersects(extent, layerExtent);
            });
            
            if (layersForLegend.length === 0) return;

            const times = layersForLegend.map(o => {
                const date = new Date(o.meta.acquisition_time);
                return date.toLocaleString(currentLang === 'fi' ? 'fi-FI' : (currentLang === 'sv' ? 'sv-SE' : (currentLang === 'de' ? 'de-DE' : 'en-GB')), { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', timeZone: 'UTC' }) + "Z";
            }).sort().reverse(); // Show newest first
            
            const sample = layersForLegend[0];
            const div = document.createElement('div'); div.style.pointerEvents = 'auto'; div.style.cursor = 'pointer';
            let html = masterLegends[id];
            
            let extraHtml = '';
            if (sample && sample.meta.resolution) extraHtml += `<div class="legend-res" style="margin-bottom: 2px;">${UI_TRANSLATIONS[currentLang].res} ${sample.meta.resolution}m/px</div>`;
            if (times.length > 0) {
                extraHtml += `<div class="legend-times" style="font-size: 9px; color: #fff; opacity: 0.8; font-family: monospace; line-height: 1.1; margin-bottom: 8px;">${times.join(' | ')}</div>`;
            }
            
            // Insert after the first </div> (which is the header title)
            const firstDivIdx = html.indexOf('</div>');
            if (firstDivIdx !== -1) {
                html = html.substring(0, firstDivIdx + 6) + `<div class="legend-meta" style="padding-top: 4px;">${extraHtml}</div>` + html.substring(firstDivIdx + 6);
            }
            div.innerHTML = html;
            div.onclick = () => { const parts = id.split('-'); const group = document.getElementById(`group-${parts[0]}`); const prod = document.getElementById(`prod-${parts[0]}-${parts.slice(1).join('-')}`); if (group) group.classList.remove('collapsed'); if (prod) prod.classList.remove('collapsed'); prod.scrollIntoView({ behavior: 'smooth', block: 'center' }); };
            panel.appendChild(div);
        }
    });
}

function updateFireOpacityArea() {
    const hasFire = Object.values(activeLayers).some(o => o.layer.getVisible() && o.meta.legend_id && o.meta.legend_id.startsWith('S3-FIRE'));
    document.getElementById('fire-opacity-area').style.display = hasFire ? 'block' : 'none';
}

function updateRadarOpacityArea() {
    const hasRadar = Object.values(activeLayers).some(o => o.layer.getVisible() && o.meta.legend_id && o.meta.legend_id.startsWith('S1-') && !o.meta.legend_id.startsWith('S1-AIS'));
    document.getElementById('radar-opacity-area').style.display = hasRadar ? 'block' : 'none';
}

async function toggleLayer(l, vis, el, isRestoring = false) {
    const path = l.path;
    if (vis) {
        if (!isRestoring && !Object.values(activeLayers).some(o => o.layer.getVisible())) {
            const b = l.bounds;
            const ext = ol.extent.boundingExtent([ol.proj.fromLonLat([b[0][1], b[0][0]]), ol.proj.fromLonLat([b[1][1], b[1][0]])]);
            if (!ol.extent.intersects(ext, map.getView().calculateExtent(map.getSize()))) map.getView().fit(ext, { padding: [50, 50, 50, 50], duration: 1000 });
        }
        if (activeLayers[path]) {
            activeLayers[path].layer.setVisible(true); el.classList.add('active');
            updateLegends(); updateGroupMarkers(); updateFireOpacityArea(); updateRadarOpacityArea(); if (!isRestoring) saveSettings(); return;
        }
        el.classList.add('active', 'loading'); document.getElementById('map-spinner').style.display = 'block';
        try {
            const source = new ol.source.GeoTIFF({
                sources: [{ url: window.location.href.split('index.html')[0].split('?')[0] + IMAGE_BASE_URL + path }],
                normalize: true,
                transition: 0,
                crossOrigin: 'anonymous'
            });
            const layer = new ol.layer.WebGLTile({ source: source, opacity: 1, visible: true });
            const ts = Math.floor((new Date(l.acquisition_time)).getTime() / 100000);
            const sat = l.satellite || '';
            let groupBase = 0;
            if (sat === 'S3') groupBase = 60000000;
            else if (sat === 'S1') groupBase = 30000000;
            layer.setZIndex(groupBase + ts);
            activeLayers[path] = { layer, meta: l };
            // Apply fire opacity if this is an S3 fire layer
            if (l.legend_id && l.legend_id.startsWith('S3-FIRE')) {
                const opacity = parseFloat(document.getElementById('fire-opacity-slider').value) / 100;
                layer.setOpacity(opacity);
            }
            // Apply radar opacity if this is an S1 radar layer (non-AIS)
            if (l.legend_id && l.legend_id.startsWith('S1-') && !l.legend_id.startsWith('S1-AIS')) {
                const opacity = parseFloat(document.getElementById('radar-opacity-slider').value) / 100;
                layer.setOpacity(opacity);
            }
            map.addLayer(layer); updateLegends(); updateGroupMarkers(); updateFireOpacityArea(); updateRadarOpacityArea(); if (!isRestoring) saveSettings();
            el.classList.remove('loading'); document.getElementById('map-spinner').style.display = 'none';
        } catch (err) { el.classList.remove('active', 'loading'); el.querySelector('input').checked = false; updateGroupMarkers(); document.getElementById('map-spinner').style.display = 'none'; }
    } else {
        el.classList.remove('active', 'loading');
        if (activeLayers[path]) { activeLayers[path].layer.setVisible(false); updateLegends(); updateGroupMarkers(); updateFireOpacityArea(); updateRadarOpacityArea(); if (!isRestoring) saveSettings(); }
    }
}
