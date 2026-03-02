let state = {
    instances: {}
};

const instanceGrid = document.getElementById('instance-grid');
const modalOverlay = document.getElementById('modal-overlay');
const instanceForm = document.getElementById('instance-form');
const toast = document.getElementById('toast');
const serverStatus = document.getElementById('server-status');

let editingId = null;

// Initialize
async function fetchData() {
    try {
        const resp = await fetch('/api/config');
        const data = await resp.json();
        state.instances = data.instances || {};
        render();
        serverStatus.textContent = 'Online';
        serverStatus.classList.add('online');
    } catch (err) {
        console.error('Failed to fetch data:', err);
        serverStatus.textContent = 'Offline';
        serverStatus.classList.remove('online');
    }
}

function render() {
    instanceGrid.innerHTML = '';

    Object.entries(state.instances).forEach(([id, info]) => {
        const card = document.createElement('div');
        card.className = 'card';

        const paramsStr = JSON.stringify(info.params || {}, null, 2);

        card.innerHTML = `
            <div class="card-header">
                <div class="card-title">${id}</div>
                <button class="btn btn-secondary btn-sm" onclick="editInstance('${id}')">Edit</button>
            </div>
            <div class="card-meta">BIN: ${info.hash?.substring(0, 8)}... | CFG: ${info.config_hash?.substring(0, 8) || 'N/A'}</div>
            <div class="card-meta">BIN URL: ${info.url?.split('/').pop()}</div>
            <div class="card-meta">CFG URL: ${info.config_url ? info.config_url.split('/').pop() : 'None'}</div>
            ${info.instance_dir ? `<div class="card-meta">DIR: ${info.instance_dir}</div>` : ''}
            <div class="card-tags">
                ${(info.tags || []).map(t => `<span class="badge">${t}</span>`).join('') || '<span class="badge badge-outline">Global</span>'}
            </div>
            <div class="card-params">${paramsStr}</div>
            <div class="card-actions">
                <button class="btn btn-danger btn-sm" onclick="deleteInstance('${id}')">Delete</button>
            </div>
        `;
        instanceGrid.appendChild(card);
    });
}

// Actions
window.editInstance = (id) => {
    editingId = id;
    const info = state.instances[id];
    document.getElementById('modal-title').textContent = 'Edit Instance';
    document.getElementById('inst-name').value = id;
    document.getElementById('inst-name').disabled = true;
    document.getElementById('inst-hash').value = info.hash || '';
    document.getElementById('inst-url').value = info.url || '';
    document.getElementById('config-hash').value = info.config_hash || '';
    document.getElementById('config-url').value = info.config_url || '';
    document.getElementById('inst-dir').value = info.instance_dir || '';
    document.getElementById('inst-args').value = JSON.stringify(info.args || [], null, 2);
    document.getElementById('inst-tags').value = (info.tags || []).join(', ');
    document.getElementById('inst-params').value = JSON.stringify(info.params || {}, null, 2);
    modalOverlay.classList.remove('hidden');
};

document.getElementById('add-instance-btn').onclick = () => {
    editingId = null;
    document.getElementById('modal-title').textContent = 'Add New Instance';
    document.getElementById('inst-name').value = '';
    document.getElementById('inst-name').disabled = false;
    document.getElementById('inst-hash').value = '';
    document.getElementById('inst-url').value = '';
    document.getElementById('config-hash').value = '';
    document.getElementById('config-url').value = '';
    document.getElementById('inst-dir').value = '';
    document.getElementById('inst-args').value = '[\n  "--config",\n  "{config}"\n]';
    document.getElementById('inst-tags').value = '';
    document.getElementById('inst-params').value = '{\n  "port": 5577\n}';
    modalOverlay.classList.remove('hidden');
};

document.getElementById('close-modal').onclick = () => {
    modalOverlay.classList.add('hidden');
};

instanceForm.onsubmit = async (e) => {
    e.preventDefault();

    const id = document.getElementById('inst-name').value;
    const hash = document.getElementById('inst-hash').value;
    const url = document.getElementById('inst-url').value;
    const configHash = document.getElementById('config-hash').value;
    const configUrl = document.getElementById('config-url').value;
    const instDir = document.getElementById('inst-dir').value;
    const tagsStr = document.getElementById('inst-tags').value;
    let params = {};
    let args = [];

    try {
        params = JSON.parse(document.getElementById('inst-params').value);
    } catch (err) {
        alert('Invalid JSON in Parameters');
        return;
    }

    try {
        args = JSON.parse(document.getElementById('inst-args').value);
        if (!Array.isArray(args)) throw new Error('Args must be an array');
    } catch (err) {
        alert('Invalid JSON in Arguments (must be a JSON array)');
        return;
    }

    const tags = tagsStr.split(',').map(t => t.trim()).filter(t => t !== "");

    // Update local state
    state.instances[id] = {
        ...state.instances[id],
        hash,
        url,
        config_hash: configHash,
        config_url: configUrl,
        instance_dir: instDir,
        args,
        tags,
        params
    };

    // Save to server
    await saveToServer();
    modalOverlay.classList.add('hidden');
    showToast('Configuration Saved');
};

window.deleteInstance = async (id) => {
    if (!confirm(`Are you sure you want to delete ${id}?`)) return;
    delete state.instances[id];
    await saveToServer();
    showToast(`${id} Deleted`);
};

async function saveToServer() {
    try {
        await fetch('/api/config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(state)
        });
        render();
    } catch (err) {
        alert('Failed to save data to server');
    }
}

function showToast(msg) {
    toast.textContent = msg;
    toast.classList.remove('hidden');
    setTimeout(() => toast.classList.add('hidden'), 3000);
}

fetchData();
setInterval(fetchData, 30000); // Polling every 30s
