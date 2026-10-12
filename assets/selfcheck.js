/* Lesson 0 self-assessment: criteria × {none, some, solid} → one summary line to paste in Teams.
   Usage:
     <div class="widget selfcheck" id="sc"></div>
     <script src="../assets/selfcheck.js"></script>
     <script>SelfCheck.render("sc", ["Python","SQL",…]);</script>
   State is kept in localStorage only. */
window.SelfCheck = (function () {
  const LEVELS = ["none", "some", "solid"];
  function render(id, criteria) {
    const root = document.getElementById(id);
    if (!root) return;
    root.classList.add("selfcheck");
    // labels only: the values in the pasted summary line stay English so the instructor can parse them
    const fr = document.documentElement.lang === "fr";
    const t = fr ? { crit: "Critère", levels: { none: "rien", some: "un peu", solid: "solide" }, ph: "ton handle GitHub", copy: "Copier la ligne de résumé", copied: "Copié" }
                 : { crit: "Criterion", levels: { none: "none", some: "some", solid: "solid" }, ph: "your GitHub handle", copy: "Copy summary line", copied: "Copied" };
    let saved = {};
    try { saved = JSON.parse(localStorage.getItem("selfcheck:" + id) || "{}"); } catch (e) {}
    const tbl = document.createElement("table");
    tbl.innerHTML = `<tr><th>${t.crit}</th>` + LEVELS.map(l => `<th>${t.levels[l]}</th>`).join("") + "</tr>";
    criteria.forEach((c, i) => {
      const tr = document.createElement("tr");
      tr.innerHTML = `<td>${c}</td>` + LEVELS.map(l =>
        `<td><input type="radio" name="${id}-${i}" value="${l}" ${saved[c] === l ? "checked" : ""} aria-label="${c}: ${l}"></td>`).join("");
      tbl.appendChild(tr);
    });
    root.appendChild(tbl);
    const handle = document.createElement("input");
    handle.placeholder = t.ph;
    handle.value = saved._handle || "";
    const lbl = document.createElement("label"); lbl.textContent = "GitHub handle"; lbl.appendChild(handle);
    root.appendChild(lbl);
    const out = document.createElement("div"); out.className = "summary"; root.appendChild(out);
    const btn = document.createElement("button"); btn.className = "btn primary"; btn.textContent = t.copy;
    btn.style.marginTop = ".6rem"; root.appendChild(btn);
    function update() {
      const state = { _handle: handle.value.trim() };
      const parts = criteria.map((c, i) => {
        const r = root.querySelector(`input[name="${id}-${i}"]:checked`);
        state[c] = r ? r.value : undefined;
        return `${c}=${r ? r.value : "?"}`;
      });
      out.textContent = `lesson0 @${state._handle || "<handle>"} ` + parts.join(" ");
      try { localStorage.setItem("selfcheck:" + id, JSON.stringify(state)); } catch (e) {}
    }
    root.addEventListener("change", update); handle.addEventListener("input", update);
    btn.addEventListener("click", () => { navigator.clipboard && navigator.clipboard.writeText(out.textContent); btn.textContent = t.copied; setTimeout(() => btn.textContent = t.copy, 1500); });
    update();
  }
  return { render };
})();
