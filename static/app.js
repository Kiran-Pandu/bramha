const state = {
  entities: [],
  selectedId: null,
  preview: null,
  graphData: null,
  graph: null,
  activeTab: "ingestion",
  liveLogs: [],
  logStreamRuns: [],
  liveRefreshTimer: null,
  liveRefreshInFlight: false,
  graphFilters: {
    kind: "all",
    minRisk: 0,
    clusterMode: "none",
    showEdgeLabels: true,
    focusNeighborhood: false,
  },
  savedSubgraphs: [],
};

const mappingFields = [
  "entity_name",
  "entity_kind",
  "entity_description",
  "entity_location",
  "entity_risk_score",
  "entity_aliases",
  "organization_name",
  "organization_location",
  "organization_risk_score",
  "organization_relationship_type",
  "event_title",
  "event_kind",
  "event_date",
  "event_location",
  "event_summary",
  "target_name",
  "target_kind",
  "target_relationship_type",
  "target_location",
];

const graphConfig = {
  width: 900,
  height: 620,
  centerX: 450,
  centerY: 310,
  charge: 3200,
  spring: 0.0018,
  damping: 0.9,
  maxVelocity: 12,
};

async function getJson(path) {
  const response = await fetch(path);
  if (!response.ok) {
    throw new Error(`Failed to load ${path}`);
  }
  return response.json();
}

async function postJson(path, payload) {
  const response = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(data.detail || `Failed to post ${path}`);
  }
  return data;
}

function riskBadge(score) {
  return `<span class="risk">${score}</span>`;
}

function card(title, subtitle, body) {
  return `
    <article class="event-card">
      <div class="event-top">
        <div>
          <h3>${title}</h3>
          <p class="chip">${subtitle}</p>
        </div>
      </div>
      ${body}
    </article>
  `;
}

function bindTabs() {
  for (const button of document.querySelectorAll("[data-tab]")) {
    button.addEventListener("click", () => {
      state.activeTab = button.dataset.tab;
      for (const tab of document.querySelectorAll("[data-tab]")) {
        tab.classList.toggle("active", tab === button);
      }
      for (const panel of document.querySelectorAll("[data-panel]")) {
        panel.classList.toggle("active", panel.dataset.panel === button.dataset.tab);
      }
      if (state.activeTab === "live-logs") {
        refreshLiveLogs();
      }
    });
  }
}

function kindColor(kind) {
  switch ((kind || "").toLowerCase()) {
    case "person":
      return "#7df9d0";
    case "organization":
      return "#ffdf8a";
    case "asset":
      return "#ff8f70";
    case "location":
      return "#83b9ff";
    default:
      return "#d5e5ff";
  }
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function stageScaleX() {
  return document.getElementById("graph-stage").clientWidth / graphConfig.width;
}

function stageScaleY() {
  return document.getElementById("graph-stage").clientHeight / graphConfig.height;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function clusterKey(node) {
  if (state.graphFilters.clusterMode === "kind") {
    return (node.kind || "unknown").toLowerCase();
  }
  if (state.graphFilters.clusterMode === "risk") {
    if (node.risk_score >= 75) {
      return "high";
    }
    if (node.risk_score >= 45) {
      return "medium";
    }
    return "low";
  }
  return "all";
}

function clusterCenter(key) {
  const centers = {
    person: { x: 250, y: 210 },
    organization: { x: 640, y: 210 },
    asset: { x: 250, y: 450 },
    location: { x: 640, y: 450 },
    high: { x: 450, y: 150 },
    medium: { x: 260, y: 430 },
    low: { x: 650, y: 430 },
    all: { x: graphConfig.centerX, y: graphConfig.centerY },
  };
  return centers[key] || centers.all;
}

function buildFilteredGraph(graph) {
  const kindFilter = state.graphFilters.kind;
  const minRisk = state.graphFilters.minRisk;
  let filteredNodes = graph.nodes.filter((node) => {
    const kindMatch = kindFilter === "all" || (node.kind || "").toLowerCase() === kindFilter;
    return kindMatch && node.risk_score >= minRisk;
  });

  if (state.graphFilters.focusNeighborhood && state.selectedId) {
    const neighborhood = new Set([state.selectedId]);
    for (const edge of graph.edges) {
      if (edge.source === state.selectedId) {
        neighborhood.add(edge.target);
      }
      if (edge.target === state.selectedId) {
        neighborhood.add(edge.source);
      }
    }
    filteredNodes = filteredNodes.filter((node) => neighborhood.has(node.id));
  }

  const allowed = new Set(filteredNodes.map((node) => node.id));
  return {
    nodes: filteredNodes,
    edges: graph.edges.filter((edge) => allowed.has(edge.source) && allowed.has(edge.target)),
  };
}

function renderStats(summary) {
  const stats = [
    ["Tracked entities", summary.entity_count],
    ["Recorded events", summary.event_count],
    ["High-risk entities", summary.high_risk_count],
    ["Sources", summary.source_count],
    ["Ingestion runs", summary.ingestion_run_count],
  ];
  document.getElementById("stats").innerHTML = stats
    .map(
      ([label, value]) => `
        <article class="stat-card">
          <span class="meta-label">${label}</span>
          <strong>${value}</strong>
        </article>
      `
    )
    .join("");

  document.getElementById("events").innerHTML = summary.latest_events
    .map(
      (event) => `
        <article class="event-card">
          <div class="event-top">
            <div>
              <h3>${event.title}</h3>
              <p class="chip">${event.kind}</p>
            </div>
            <span class="meta-label">${event.event_date}</span>
          </div>
          <p>${event.summary}</p>
          <p class="meta-label">${event.location}</p>
        </article>
      `
    )
    .join("");
}

function renderRuns(runs) {
  const container = document.getElementById("runs");
  container.innerHTML = runs.length
    ? runs
        .map((run) =>
          card(
            run.source_name,
            `${run.status} - ${run.file_type.toUpperCase()}`,
            `
              <p class="meta-label">${run.file_name}</p>
              <p>records ${run.record_count}</p>
              <p>entities new ${run.entity_count} - matched ${run.entity_existing_count ?? 0}</p>
              <p>events new ${run.event_count} - matched ${run.event_existing_count ?? 0}</p>
              <p>relationships new ${run.relationship_count} - existing ${run.relationship_existing_count ?? 0}</p>
              <p class="meta-label">${run.created_at}</p>
              ${run.error_count ? `<p class="error-text">Errors: ${run.error_count}</p>` : ""}
            `
          )
        )
        .join("")
    : `<p class="meta-label">No ingestion runs yet.</p>`;
}

function renderSources(sources) {
  const container = document.getElementById("sources");
  container.innerHTML = sources.length
    ? sources
        .map((source) =>
          card(
            source.name,
            source.file_type.toUpperCase(),
            `
              <p>entities ${source.entity_count} - events ${source.event_count}</p>
              <p class="meta-label">Last run ${source.last_run_at}</p>
            `
          )
        )
        .join("")
    : `<p class="meta-label">No sources tracked yet.</p>`;
}

function renderLogStreamRuns(runs) {
  const container = document.getElementById("log-stream-runs");
  container.innerHTML = runs.length
    ? runs
        .map((run) =>
          card(
            run.source_name,
            `${run.status} - ${run.topic}`,
            `
              <p>messages ${run.message_count}</p>
              <p>entities ${run.created_entity_count} - events ${run.created_event_count}</p>
              <p>relationships ${run.created_relationship_count}</p>
              <p class="meta-label">Updated ${run.updated_at}</p>
            `
          )
        )
        .join("")
    : `<p class="meta-label">No Kafka stream runs yet.</p>`;
}

function renderLiveLogs(logs) {
  const container = document.getElementById("live-log-feed");
  container.innerHTML = logs.length
    ? logs
        .map((entry) => {
          const parsed = entry.parsed || {};
          return `
            <article class="live-log-card">
              <div class="live-log-top">
                <div>
                  <strong>${escapeHtml(parsed.title || parsed.service || "Log event")}</strong>
                  <p class="meta-label">${escapeHtml(parsed.timestamp || entry.created_at)} - ${escapeHtml(parsed.host || "unknown host")}</p>
                </div>
                <span class="chip">${escapeHtml(parsed.service || "unknown service")}</span>
              </div>
              <p>${escapeHtml(parsed.message || entry.raw_line)}</p>
              <div class="live-log-meta">
                <span class="chip">line ${entry.line_number}</span>
                <span class="chip">offset ${entry.offset_value}</span>
                <span class="chip">${escapeHtml(entry.topic)}</span>
              </div>
              <pre class="live-log-raw">${escapeHtml(entry.raw_line)}</pre>
            </article>
          `;
        })
        .join("")
    : `<p class="meta-label">No streamed logs received yet.</p>`;
}

function renderEntityList() {
  const container = document.getElementById("entity-list");
  container.innerHTML = state.entities
    .map(
      (entity) => `
        <article class="entity-card ${entity.id === state.selectedId ? "active" : ""}" data-id="${entity.id}">
          <div class="entity-top">
            <div>
              <h3>${entity.name}</h3>
              <p class="chip">${entity.kind}</p>
            </div>
            ${riskBadge(entity.risk_score)}
          </div>
          <p>${entity.description}</p>
          <p class="meta-label">${entity.location}</p>
        </article>
      `
    )
    .join("");

  for (const cardElement of container.querySelectorAll(".entity-card")) {
    cardElement.addEventListener("click", () => selectEntity(cardElement.dataset.id));
  }
}

function renderDetail(entity) {
  const metadata = Object.entries(entity.metadata || {})
    .map(([key, value]) => `<span class="chip">${key}: ${Array.isArray(value) ? value.join(", ") : value}</span>`)
    .join("");
  const aliases = entity.aliases?.length
    ? entity.aliases.map((alias) => `<span class="chip">${alias}</span>`).join("")
    : `<span class="meta-label">No aliases recorded.</span>`;
  const provenance = entity.sources?.length
    ? entity.sources
        .map((source) => `<span class="chip">${source.name} - ${source.file_type} - ${source.first_seen_at}</span>`)
        .join("")
    : `<span class="meta-label">No source provenance yet.</span>`;

  const events = entity.events
    .map(
      (event) => `
        <article class="event-card timeline-item">
          <div class="event-top">
            <div>
              <h3>${event.title}</h3>
              <p class="chip">${event.kind}</p>
            </div>
            <span class="meta-label">${event.event_date}</span>
          </div>
          <p>${event.summary}</p>
          <p class="meta-label">${event.location} - role: ${event.role}</p>
        </article>
      `
    )
    .join("");

  const links = entity.links?.length
    ? entity.links
        .map(
          (link) => `
            <article class="link-card">
              <div class="link-top">
                <div>
                  <h3>${link.name}</h3>
                  <p class="chip">${link.kind}</p>
                </div>
                ${riskBadge(link.risk_score)}
              </div>
              <p class="meta-label">${link.relationship_type} - strength ${link.strength}</p>
              <p>${link.source_event_id ? `Source event ${link.source_event_id}` : "Direct entity relationship"}</p>
            </article>
          `
        )
        .join("")
    : `<p class="meta-label">No linked entities found.</p>`;

  document.getElementById("entity-detail").innerHTML = `
    <div class="detail-top">
      <div>
        <p class="eyebrow">Entity Brief</p>
        <h2>${entity.name}</h2>
        <p>${entity.description}</p>
      </div>
      ${riskBadge(entity.risk_score)}
    </div>
    <div class="chip-row">
      <span class="chip">${entity.kind}</span>
      <span class="chip">${entity.location}</span>
      ${metadata}
    </div>
    <section class="detail-section">
      <h3>Aliases</h3>
      <div class="chip-row">${aliases}</div>
    </section>
    <section class="detail-section">
      <h3>Source Provenance</h3>
      <div class="chip-row">${provenance}</div>
    </section>
    <section class="detail-section">
      <h3>Event Timeline</h3>
      <div class="timeline">${events}</div>
    </section>
    <section class="detail-section">
      <h3>Relationship Links</h3>
      <div class="links">${links}</div>
    </section>
  `;
}

function buildGraphState(graph) {
  const nodes = graph.nodes.map((node, index) => ({
    ...node,
    x: clusterCenter(clusterKey(node)).x + Math.cos(index * 1.7) * (120 + (index % 5) * 35),
    y: clusterCenter(clusterKey(node)).y + Math.sin(index * 1.7) * (120 + (index % 5) * 35),
    vx: 0,
    vy: 0,
    radius: 10 + Math.round(node.risk_score / 18),
    neighbors: new Set(),
    degree: 0,
    cluster: clusterKey(node),
  }));
  const nodeMap = new Map(nodes.map((node) => [node.id, node]));
  const edges = graph.edges
    .map((edge, index) => {
      const source = nodeMap.get(edge.source);
      const target = nodeMap.get(edge.target);
      if (!source || !target) {
        return null;
      }
      source.neighbors.add(target.id);
      target.neighbors.add(source.id);
      source.degree += edge.weight;
      target.degree += edge.weight;
      return { ...edge, index, sourceNode: source, targetNode: target };
    })
    .filter(Boolean);
  return {
    nodes,
    edges,
    nodeMap,
    scale: 1,
    translateX: 0,
    translateY: 0,
    animationFrame: null,
    draggingNode: null,
    panning: false,
    panStartX: 0,
    panStartY: 0,
    selectedNodeId: state.selectedId,
    hoveredNodeId: null,
  };
}

function applyGraphTransform() {
  if (!state.graph) {
    return;
  }
  const viewport = document.getElementById("graph-viewport");
  viewport.setAttribute(
    "transform",
    `translate(${state.graph.translateX} ${state.graph.translateY}) scale(${state.graph.scale})`
  );
}

function renderGraphInspector(node) {
  const inspector = document.getElementById("graph-inspector");
  if (!node) {
    inspector.innerHTML = "Select a node to inspect its local network.";
    return;
  }
  const neighbors = [...node.neighbors]
    .map((id) => state.graph.nodeMap.get(id))
    .filter(Boolean)
    .sort((a, b) => b.risk_score - a.risk_score || a.name.localeCompare(b.name))
    .slice(0, 8);
  inspector.innerHTML = `
    <p class="eyebrow">Focused Node</p>
    <h3>${escapeHtml(node.name)}</h3>
    <p class="meta-label">${escapeHtml(node.kind)} - risk ${node.risk_score} - degree ${node.degree}</p>
    <div class="chip-row">
      ${neighbors.length ? neighbors.map((neighbor) => `<span class="chip">${escapeHtml(neighbor.name)}</span>`).join("") : '<span class="meta-label">No connected neighbors.</span>'}
    </div>
  `;
}

function renderSavedSubgraphs() {
  const container = document.getElementById("saved-subgraphs");
  container.innerHTML = state.savedSubgraphs.length
    ? state.savedSubgraphs
        .map(
          (item) => `
            <article class="subgraph-card" data-subgraph-id="${item.id}">
              <strong>${escapeHtml(item.name)}</strong>
              <p class="meta-label">${item.graph.nodes.length} nodes - ${item.graph.edges.length} edges</p>
              <p class="meta-label">${item.created_at}</p>
            </article>
          `
        )
        .join("")
    : `<p class="meta-label">No saved subgraphs yet.</p>`;

  for (const cardElement of container.querySelectorAll(".subgraph-card")) {
    cardElement.addEventListener("click", async () => {
      const subgraph = state.savedSubgraphs.find((item) => item.id === cardElement.dataset.subgraphId);
      if (!subgraph) {
        return;
      }
      applyGraphFilters(subgraph.filters || {});
      renderGraph(subgraph.graph);
      if (subgraph.focus_entity_id) {
        await selectEntity(subgraph.focus_entity_id);
      }
    });
  }
}

function showGraphTooltip(node, event) {
  const tooltip = document.getElementById("graph-tooltip");
  tooltip.classList.remove("hidden");
  tooltip.innerHTML = `
    <strong>${escapeHtml(node.name)}</strong>
    <p class="meta-label">${escapeHtml(node.kind)} - risk ${node.risk_score}</p>
    <p class="meta-label">Neighbors ${node.neighbors.size}</p>
  `;
  const bounds = document.getElementById("graph-stage").getBoundingClientRect();
  tooltip.style.left = `${clamp(event.clientX - bounds.left + 14, 12, bounds.width - 220)}px`;
  tooltip.style.top = `${clamp(event.clientY - bounds.top + 14, 12, bounds.height - 120)}px`;
}

function hideGraphTooltip() {
  document.getElementById("graph-tooltip").classList.add("hidden");
}

function updateGraphHighlight() {
  if (!state.graph) {
    return;
  }
  const focusId = state.graph.selectedNodeId || state.graph.hoveredNodeId;
  const activeSet = new Set();
  if (focusId && state.graph.nodeMap.has(focusId)) {
    activeSet.add(focusId);
    for (const neighborId of state.graph.nodeMap.get(focusId).neighbors) {
      activeSet.add(neighborId);
    }
  }

  for (const node of state.graph.nodes) {
    node.element.classList.toggle("focused", node.id === state.graph.selectedNodeId);
    node.element.classList.toggle("dimmed", activeSet.size > 0 && !activeSet.has(node.id));
  }
  for (const edge of state.graph.edges) {
    const active =
      focusId &&
      (edge.source === focusId || edge.target === focusId || (activeSet.has(edge.source) && activeSet.has(edge.target)));
    edge.element.classList.toggle("active", Boolean(active));
    edge.element.classList.toggle("dimmed", activeSet.size > 0 && !active);
    if (edge.labelElement) {
      edge.labelElement.classList.toggle("dimmed", activeSet.size > 0 && !active);
    }
  }

  renderGraphInspector(focusId ? state.graph.nodeMap.get(focusId) : null);
}

function tickGraph() {
  if (!state.graph) {
    return;
  }
  const { nodes, edges } = state.graph;
  let totalMotion = 0;

  for (let i = 0; i < nodes.length; i += 1) {
    const nodeA = nodes[i];
    if (state.graph.draggingNode?.id === nodeA.id) {
      continue;
    }
    for (let j = i + 1; j < nodes.length; j += 1) {
      const nodeB = nodes[j];
      const dx = nodeB.x - nodeA.x;
      const dy = nodeB.y - nodeA.y;
      const distanceSquared = dx * dx + dy * dy + 0.1;
      const distance = Math.sqrt(distanceSquared);
      const force = graphConfig.charge / distanceSquared;
      const fx = (dx / distance) * force;
      const fy = (dy / distance) * force;
      nodeA.vx -= fx;
      nodeA.vy -= fy;
      nodeB.vx += fx;
      nodeB.vy += fy;
    }
  }

  for (const edge of edges) {
    const dx = edge.targetNode.x - edge.sourceNode.x;
    const dy = edge.targetNode.y - edge.sourceNode.y;
    const distance = Math.sqrt(dx * dx + dy * dy) || 1;
    const targetDistance = 70 + edge.weight * 18;
    const stretch = distance - targetDistance;
    const force = stretch * graphConfig.spring;
    const fx = (dx / distance) * force;
    const fy = (dy / distance) * force;
    edge.sourceNode.vx += fx;
    edge.sourceNode.vy += fy;
    edge.targetNode.vx -= fx;
    edge.targetNode.vy -= fy;
  }

  for (const node of nodes) {
    if (state.graph.draggingNode?.id === node.id) {
      continue;
    }
    const center = clusterCenter(node.cluster);
    node.vx += (center.x - node.x) * 0.0011;
    node.vy += (center.y - node.y) * 0.0011;
    node.vx *= graphConfig.damping;
    node.vy *= graphConfig.damping;
    node.vx = clamp(node.vx, -graphConfig.maxVelocity, graphConfig.maxVelocity);
    node.vy = clamp(node.vy, -graphConfig.maxVelocity, graphConfig.maxVelocity);
    node.x += node.vx;
    node.y += node.vy;
    totalMotion += Math.abs(node.vx) + Math.abs(node.vy);
  }

  for (const edge of edges) {
    edge.element.setAttribute("x1", edge.sourceNode.x);
    edge.element.setAttribute("y1", edge.sourceNode.y);
    edge.element.setAttribute("x2", edge.targetNode.x);
    edge.element.setAttribute("y2", edge.targetNode.y);
    if (edge.labelElement) {
      edge.labelElement.setAttribute("x", String((edge.sourceNode.x + edge.targetNode.x) / 2));
      edge.labelElement.setAttribute("y", String((edge.sourceNode.y + edge.targetNode.y) / 2 - 6));
    }
  }

  for (const node of nodes) {
    node.element.setAttribute("transform", `translate(${node.x} ${node.y})`);
  }

  if (totalMotion > 0.3 || state.graph.draggingNode) {
    state.graph.animationFrame = window.requestAnimationFrame(tickGraph);
  } else {
    state.graph.animationFrame = null;
  }
}

function ensureGraphAnimation() {
  if (state.graph && !state.graph.animationFrame) {
    state.graph.animationFrame = window.requestAnimationFrame(tickGraph);
  }
}

function focusGraphNode(nodeId) {
  if (!state.graph || !state.graph.nodeMap.has(nodeId)) {
    return;
  }
  state.graph.selectedNodeId = nodeId;
  updateGraphHighlight();
}

function toGraphPoint(clientX, clientY) {
  const stage = document.getElementById("graph-stage").getBoundingClientRect();
  const x = (clientX - stage.left) / stageScaleX();
  const y = (clientY - stage.top) / stageScaleY();
  return {
    x: (x - state.graph.translateX) / state.graph.scale,
    y: (y - state.graph.translateY) / state.graph.scale,
  };
}

function fitGraphToView() {
  if (!state.graph || state.graph.nodes.length === 0) {
    return;
  }
  const xs = state.graph.nodes.map((node) => node.x);
  const ys = state.graph.nodes.map((node) => node.y);
  const minX = Math.min(...xs);
  const maxX = Math.max(...xs);
  const minY = Math.min(...ys);
  const maxY = Math.max(...ys);
  const width = Math.max(140, maxX - minX);
  const height = Math.max(140, maxY - minY);
  const scale = clamp(Math.min((graphConfig.width - 120) / width, (graphConfig.height - 120) / height), 0.55, 1.8);
  state.graph.scale = scale;
  state.graph.translateX = graphConfig.centerX - ((minX + maxX) / 2) * scale;
  state.graph.translateY = graphConfig.centerY - ((minY + maxY) / 2) * scale;
  applyGraphTransform();
}

function resetGraphPhysics() {
  if (!state.graphData) {
    return;
  }
  renderGraph(buildFilteredGraph(state.graphData));
}

function bindGraphInteractions() {
  const svg = document.getElementById("graph-svg");
  const stage = document.getElementById("graph-stage");

  svg.addEventListener("wheel", (event) => {
    event.preventDefault();
    if (!state.graph) {
      return;
    }
    const delta = event.deltaY < 0 ? 1.1 : 0.92;
    const nextScale = clamp(state.graph.scale * delta, 0.35, 2.6);
    const point = toGraphPoint(event.clientX, event.clientY);
    state.graph.translateX = event.offsetX / stageScaleX() - point.x * nextScale;
    state.graph.translateY = event.offsetY / stageScaleY() - point.y * nextScale;
    state.graph.scale = nextScale;
    applyGraphTransform();
  });

  svg.addEventListener("pointerdown", (event) => {
    if (!state.graph) {
      return;
    }
    if (event.target.closest(".graph-node")) {
      return;
    }
    state.graph.panning = true;
    state.graph.panStartX = event.clientX / stageScaleX() - state.graph.translateX;
    state.graph.panStartY = event.clientY / stageScaleY() - state.graph.translateY;
    hideGraphTooltip();
  });

  window.addEventListener("pointermove", (event) => {
    if (state.graph?.draggingNode) {
      const point = toGraphPoint(event.clientX, event.clientY);
      state.graph.draggingNode.x = point.x;
      state.graph.draggingNode.y = point.y;
      state.graph.draggingNode.vx = 0;
      state.graph.draggingNode.vy = 0;
      ensureGraphAnimation();
      return;
    }

    if (state.graph?.panning) {
      state.graph.translateX = event.clientX / stageScaleX() - state.graph.panStartX;
      state.graph.translateY = event.clientY / stageScaleY() - state.graph.panStartY;
      applyGraphTransform();
    }
  });

  window.addEventListener("pointerup", () => {
    if (state.graph?.draggingNode) {
      state.graph.draggingNode = null;
      ensureGraphAnimation();
    }
    if (state.graph) {
      state.graph.panning = false;
    }
  });

  document.getElementById("graph-fit-button").addEventListener("click", fitGraphToView);
  document.getElementById("graph-reset-button").addEventListener("click", resetGraphPhysics);
  document.getElementById("graph-save-button").addEventListener("click", saveCurrentSubgraph);
}

function renderGraph(graph) {
  document.getElementById("graph-density").textContent = `${graph.edges.length}`;
  if (state.graph?.animationFrame) {
    window.cancelAnimationFrame(state.graph.animationFrame);
  }
  state.graph = buildGraphState(graph);

  const linksRoot = document.getElementById("graph-links");
  const labelsRoot = document.getElementById("graph-edge-labels");
  const nodesRoot = document.getElementById("graph-nodes");
  linksRoot.innerHTML = "";
  labelsRoot.innerHTML = "";
  nodesRoot.innerHTML = "";

  for (const edge of state.graph.edges) {
    const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
    line.setAttribute("class", "graph-link");
    line.setAttribute("stroke-width", String(1.4 + edge.weight * 0.65));
    edge.element = line;
    linksRoot.appendChild(line);

    const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
    label.setAttribute("class", `graph-edge-label${state.graphFilters.showEdgeLabels ? "" : " hidden"}`);
    label.textContent = edge.relationship_type;
    edge.labelElement = label;
    labelsRoot.appendChild(label);
  }

  for (const node of state.graph.nodes) {
    const group = document.createElementNS("http://www.w3.org/2000/svg", "g");
    group.setAttribute("class", "graph-node");

    const circle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
    circle.setAttribute("r", String(node.radius));
    circle.setAttribute("fill", kindColor(node.kind));

    const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
    label.setAttribute("x", String(node.radius + 8));
    label.setAttribute("y", "4");
    label.textContent = node.name;

    group.appendChild(circle);
    group.appendChild(label);
    node.element = group;
    nodesRoot.appendChild(group);

    group.addEventListener("pointerenter", (event) => {
      state.graph.hoveredNodeId = node.id;
      updateGraphHighlight();
      showGraphTooltip(node, event);
    });
    group.addEventListener("pointermove", (event) => {
      showGraphTooltip(node, event);
    });
    group.addEventListener("pointerleave", () => {
      state.graph.hoveredNodeId = null;
      updateGraphHighlight();
      hideGraphTooltip();
    });
    group.addEventListener("click", async (event) => {
      event.stopPropagation();
      await selectEntity(node.id);
      focusGraphNode(node.id);
    });
    group.addEventListener("pointerdown", (event) => {
      event.stopPropagation();
      state.graph.draggingNode = node;
    });
  }

  applyGraphTransform();
  updateGraphHighlight();
  ensureGraphAnimation();
  window.setTimeout(fitGraphToView, 280);
}

function renderMapping(preview) {
  state.preview = preview;
  const panel = document.getElementById("mapping-panel");
  panel.classList.remove("hidden");
  document.getElementById("preview-summary").textContent = `${preview.record_count} records detected`;
  document.getElementById("preview-sample").textContent = JSON.stringify(preview.sample_records, null, 2);
  document.getElementById("mapping-grid").innerHTML = mappingFields
    .map((field) => {
      const options = ['<option value="">Ignore</option>']
        .concat(
          preview.fields.map(
            (sourceField) =>
              `<option value="${sourceField}" ${preview.suggested_mapping[field] === sourceField ? "selected" : ""}>${sourceField}</option>`
          )
        )
        .join("");
      return `
        <label class="mapping-field">
          <span class="meta-label">${field}</span>
          <select data-mapping-field="${field}">${options}</select>
        </label>
      `;
    })
    .join("");
}

function collectMapping() {
  const mapping = {};
  for (const select of document.querySelectorAll("[data-mapping-field]")) {
    if (select.value) {
      mapping[select.dataset.mappingField] = select.value;
    }
  }
  return mapping;
}

function applyGraphFilters(filters) {
  state.graphFilters = {
    ...state.graphFilters,
    ...filters,
  };
  document.getElementById("graph-kind-filter").value = state.graphFilters.kind;
  document.getElementById("graph-risk-filter").value = String(state.graphFilters.minRisk);
  document.getElementById("graph-risk-value").textContent = `${state.graphFilters.minRisk}+`;
  document.getElementById("graph-cluster-mode").value = state.graphFilters.clusterMode;
  document.getElementById("graph-label-toggle").checked = state.graphFilters.showEdgeLabels;
  document.getElementById("graph-focus-toggle").checked = state.graphFilters.focusNeighborhood;
}

function currentGraphPayload() {
  return {
    nodes: state.graph.nodes.map(({ id, name, kind, risk_score }) => ({ id, name, kind, risk_score })),
    edges: state.graph.edges.map(({ source, target, relationship_type, weight }) => ({
      source,
      target,
      relationship_type,
      weight,
    })),
  };
}

async function saveCurrentSubgraph() {
  if (!state.graph) {
    return;
  }
  const fallbackName = state.selectedId ? `Focus ${state.selectedId}` : `Subgraph ${new Date().toLocaleTimeString()}`;
  const name = window.prompt("Name this subgraph", fallbackName);
  if (!name) {
    return;
  }
  const saved = await postJson("/api/investigations/subgraphs", {
    name,
    focus_entity_id: state.selectedId || "",
    filters: state.graphFilters,
    graph: currentGraphPayload(),
  });
  state.savedSubgraphs = [saved, ...state.savedSubgraphs].slice(0, 20);
  renderSavedSubgraphs();
}

async function loadEntities(search = "") {
  state.entities = await getJson(`/api/entities${search ? `?search=${encodeURIComponent(search)}` : ""}`);
  if (!state.selectedId && state.entities[0]) {
    state.selectedId = state.entities[0].id;
  }
  if (state.selectedId && !state.entities.some((entity) => entity.id === state.selectedId)) {
    state.selectedId = state.entities[0]?.id ?? null;
  }
  renderEntityList();
  if (state.selectedId) {
    await selectEntity(state.selectedId, false);
  } else {
    document.getElementById("entity-detail").textContent = "No entities matched your search.";
  }
}

async function selectEntity(entityId, redrawList = true) {
  state.selectedId = entityId;
  if (redrawList) {
    renderEntityList();
  }
  const entity = await getJson(`/api/entities/${entityId}`);
  renderDetail(entity);
  if (state.graphData) {
    renderGraph(buildFilteredGraph(state.graphData));
    if (state.graph?.nodeMap.has(entityId)) {
      focusGraphNode(entityId);
    }
  }
}

async function refreshDashboard() {
  const [summary, graph, runs, sources, savedSubgraphs] = await Promise.all([
    getJson("/api/summary"),
    getJson("/api/graph"),
    getJson("/api/ingest/runs"),
    getJson("/api/sources"),
    getJson("/api/investigations/subgraphs"),
  ]);
  state.graphData = graph;
  state.savedSubgraphs = savedSubgraphs;
  renderStats(summary);
  renderGraph(buildFilteredGraph(graph));
  renderRuns(runs);
  renderSources(sources);
  renderSavedSubgraphs();
}

async function refreshLiveLogs() {
  if (state.liveRefreshInFlight) {
    return;
  }
  state.liveRefreshInFlight = true;
  const status = document.getElementById("live-log-status");
  try {
    const [runs, logs] = await Promise.all([
      getJson("/api/logs/runs?limit=8"),
      getJson("/api/logs/recent?limit=40"),
    ]);
    state.logStreamRuns = runs;
    state.liveLogs = logs;
    renderLogStreamRuns(runs);
    renderLiveLogs(logs);
    status.textContent = `Live - ${logs.length} recent log lines`;
    await refreshDashboard();
  } catch (error) {
    status.textContent = `Live refresh failed: ${error.message}`;
  } finally {
    state.liveRefreshInFlight = false;
  }
}

function startLiveRefreshLoop() {
  if (state.liveRefreshTimer) {
    window.clearInterval(state.liveRefreshTimer);
  }
  state.liveRefreshTimer = window.setInterval(() => {
    if (state.activeTab === "live-logs") {
      refreshLiveLogs();
    }
  }, 3000);
}

async function readSelectedFile() {
  const fileInput = document.getElementById("file-input");
  const file = fileInput.files?.[0];
  if (!file) {
    throw new Error("Choose a file first.");
  }

  const extension = file.name.split(".").pop()?.toLowerCase() || "";
  if (!["csv", "json"].includes(extension)) {
    throw new Error("Only CSV and JSON files are supported right now.");
  }

  const content = await file.text();
  return { file, extension, content };
}

async function handlePreview() {
  const status = document.getElementById("ingest-status");
  try {
    const { file, extension, content } = await readSelectedFile();
    status.textContent = `Previewing ${file.name}...`;
    const preview = await postJson("/api/ingest/preview", {
      file_name: file.name,
      file_type: extension,
      content,
    });
    renderMapping(preview);
    status.textContent = `Preview ready for ${file.name}. Adjust mappings if needed, then run ingestion.`;
  } catch (error) {
    status.textContent = error.message;
  }
}

async function handleIngest(event) {
  event.preventDefault();
  const sourceName = document.getElementById("source-name").value.trim();
  const status = document.getElementById("ingest-status");
  if (!sourceName) {
    status.textContent = "Enter a source name first.";
    return;
  }

  document.getElementById("ingest-button").disabled = true;

  try {
    const { file, extension, content } = await readSelectedFile();
    if (!state.preview) {
      const preview = await postJson("/api/ingest/preview", {
        file_name: file.name,
        file_type: extension,
        content,
      });
      renderMapping(preview);
    }

    const run = await postJson("/api/ingest/run", {
      source_name: sourceName,
      file_name: file.name,
      file_type: extension,
      content,
      mapping: collectMapping(),
    });
    status.textContent = `Ingestion completed: new entities ${run.entity_count}, matched entities ${run.entity_existing_count ?? 0}, new events ${run.event_count}, matched events ${run.event_existing_count ?? 0}, new relationships ${run.relationship_count}.`;
    await refreshDashboard();
    await loadEntities(document.getElementById("search").value.trim());
  } catch (error) {
    status.textContent = error.message;
  } finally {
    document.getElementById("ingest-button").disabled = false;
  }
}

function bindGraphControls() {
  document.getElementById("graph-kind-filter").addEventListener("change", (event) => {
    state.graphFilters.kind = event.target.value;
    renderGraph(buildFilteredGraph(state.graphData));
  });
  document.getElementById("graph-risk-filter").addEventListener("input", (event) => {
    state.graphFilters.minRisk = Number(event.target.value);
    document.getElementById("graph-risk-value").textContent = `${state.graphFilters.minRisk}+`;
    renderGraph(buildFilteredGraph(state.graphData));
  });
  document.getElementById("graph-cluster-mode").addEventListener("change", (event) => {
    state.graphFilters.clusterMode = event.target.value;
    renderGraph(buildFilteredGraph(state.graphData));
  });
  document.getElementById("graph-label-toggle").addEventListener("change", (event) => {
    state.graphFilters.showEdgeLabels = event.target.checked;
    renderGraph(buildFilteredGraph(state.graphData));
  });
  document.getElementById("graph-focus-toggle").addEventListener("change", (event) => {
    state.graphFilters.focusNeighborhood = event.target.checked;
    renderGraph(buildFilteredGraph(state.graphData));
  });
}

async function init() {
  bindTabs();
  bindGraphInteractions();
  bindGraphControls();
  applyGraphFilters(state.graphFilters);
  await refreshDashboard();
  await loadEntities();
  startLiveRefreshLoop();

  document.getElementById("search").addEventListener("input", async (event) => {
    await loadEntities(event.target.value.trim());
  });
  document.getElementById("preview-button").addEventListener("click", handlePreview);
  document.getElementById("ingest-form").addEventListener("submit", handleIngest);
  document.getElementById("live-refresh-button").addEventListener("click", refreshLiveLogs);
}

init().catch((error) => {
  document.body.innerHTML = `<pre>${error.message}</pre>`;
});
