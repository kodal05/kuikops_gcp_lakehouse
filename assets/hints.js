/* Hint toggles for exercises.
   Every exercise ships two hints and a solution pointer behind reveal toggles
   (docs/adr/0001). Usage:
     <div class="hints" id="ex1"></div>
     <script src="../assets/hints.js"></script>
     <script>Hints.render("ex1", {
       hint1: "<p>…</p>", hint2: "<p>…</p>",
       solution: "gs://kuikops-lakehouse-solutions/day1/…",  // a pointer, never the code
       unlock: "19:00"                                        // optional, default 17:00
     });</script>
   Each opened toggle is remembered in localStorage so a learner can see, honestly, how
   much help they used. Nothing is sent anywhere. */
window.Hints = (function () {
  function el(tag, attrs, html) {
    const n = document.createElement(tag);
    for (const k in (attrs || {})) n.setAttribute(k, attrs[k]);
    if (html) n.innerHTML = html;
    return n;
  }
  function remember(key) { try { localStorage.setItem("hint:" + key, "1"); } catch (e) {} }
  function render(id, h) {
    const root = document.getElementById(id);
    if (!root) return;
    root.classList.add("hints");
    const fr = document.documentElement.lang === "fr";
    const unlock = h.unlock || "17:00"; // course rule; a lesson may pass a later time
    const items = fr ? [
      ["Indice 1", h.hint1, "un coup de pouce"],
      ["Indice 2", h.hint2, "la forme de la réponse"],
      ["Où est la solution", h.solution ? `<p>Solution de référence (lisible à tout moment, chaque lecture est tracée par apprenant)\u202f: <code>${h.solution}</code><br>Règle du cours\u202f: ouvre-la après ${unlock.replace(":", "h")} et après les deux indices, et dis-le dans la PR.</p>` : "", "où trouver la solution de référence"]
    ] : [
      ["Hint 1", h.hint1, "a nudge"],
      ["Hint 2", h.hint2, "the shape of the answer"],
      ["Solution pointer", h.solution ? `<p>Reference solution (readable any time, every read is logged per learner): <code>${h.solution}</code><br>Course rule: open it after ${unlock} and after both hints, and say so in the PR.</p>` : "", "where the reference solution lives"]
    ];
    items.forEach(([label, body, sub], i) => {
      if (!body) return;
      const d = el("details", { class: "hint" });
      d.appendChild(el("summary", {}, `<strong>${label}</strong> <span class="sub">— ${sub}</span>`));
      d.appendChild(el("div", { class: "hint-body" }, body));
      d.addEventListener("toggle", () => { if (d.open) remember(id + ":" + i); });
      root.appendChild(d);
    });
  }
  return { render };
})();
