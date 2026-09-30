// Sortable tables: add data-sortable to <table> and data-sort="text|number" to <th>.
document.querySelectorAll("table[data-sortable]").forEach((table) => {
  const headers = table.querySelectorAll("thead th");
  headers.forEach((th, index) => {
    if (!th.dataset.sort) return;
    th.addEventListener("click", () => {
      const dir = th.dataset.dir === "desc" ? "asc" : "desc";
      headers.forEach((h) => delete h.dataset.dir);
      th.dataset.dir = dir;
      const body = table.tBodies[0];
      const value = (row) => {
        const cell = row.children[index];
        const raw = cell ? (cell.dataset.value ?? cell.innerText.trim()) : "";
        return th.dataset.sort === "number" ? parseFloat(String(raw).replace(/[^0-9.-]/g, "")) || 0 : raw.toLowerCase();
      };
      const rows = Array.from(body.rows);
      rows.sort((a, b) => {
        const av = value(a), bv = value(b);
        if (av === bv) return 0;
        return (av > bv ? 1 : -1) * (dir === "asc" ? 1 : -1);
      });
      rows.forEach((r) => body.appendChild(r));
    });
  });
});

// Line/bar chart comparing a period with the same period last year.
window.renderComparisonChart = function (canvasId, dataId, type) {
  const el = document.getElementById(canvasId);
  const data = JSON.parse(document.getElementById(dataId).textContent);
  if (!el || !window.Chart) return;
  const styles = getComputedStyle(document.documentElement);
  const primary = styles.getPropertyValue("--color-primary").trim() || "#2563eb";
  const muted = styles.getPropertyValue("--color-base-300").trim() || "#cbd5e1";
  const fmt = new Intl.NumberFormat(undefined, { style: "currency", currency: data.currency, maximumFractionDigits: 0 });
  new Chart(el, {
    type: type || "bar",
    data: {
      labels: data.labels,
      datasets: [
        { label: data.previous_label, data: data.previous, backgroundColor: muted, borderColor: muted, borderRadius: 4 },
        { label: data.current_label, data: data.current, backgroundColor: primary, borderColor: primary, borderRadius: 4 },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { position: "bottom", labels: { usePointStyle: true } },
        tooltip: { callbacks: { label: (ctx) => `${ctx.dataset.label}: ${fmt.format(ctx.parsed.y ?? 0)}` } },
      },
      scales: {
        x: { grid: { display: false } },
        y: { beginAtZero: true, ticks: { callback: (v) => fmt.format(v) } },
      },
    },
  });
};

// Copy-to-clipboard buttons: <button data-copy="text">.
document.querySelectorAll("[data-copy]").forEach((btn) => {
  btn.addEventListener("click", async () => {
    await navigator.clipboard.writeText(btn.dataset.copy);
    const original = btn.innerText;
    btn.innerText = "Copied!";
    setTimeout(() => (btn.innerText = original), 1500);
  });
});
