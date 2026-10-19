/* Retrieval-practice quiz widget.
   Usage:
     <div class="widget quiz" id="quiz1"></div>
     <script src="../assets/quiz.js"></script>
     <script>Quiz.render("quiz1", [{ prompt, options:[...], answer: <index>, explain }, ...]);</script>
   Options are hidden until the learner clicks "Show options" — think first, then recognise. */
window.Quiz = (function () {
  function el(tag, attrs, children) {
    const n = document.createElement(tag);
    for (const k in (attrs || {})) {
      if (k === "html") n.innerHTML = attrs[k];
      else if (k === "text") n.textContent = attrs[k];
      else n.setAttribute(k, attrs[k]);
    }
    (children || []).forEach(c => n.appendChild(c));
    return n;
  }
  function render(id, questions) {
    const root = document.getElementById(id);
    if (!root) return;
    root.classList.add("quiz");
    const state = { answered: 0, correct: 0 };
    const t = document.documentElement.lang === "fr" ? {
      title: "Teste-toi", intro: "Réponds à chaque question dans ta tête <em>avant</em> d'afficher les choix. Deviner à partir des choix, ça donne l'impression de savoir. Se rappeler, c'est ce qui fait retenir.",
      show: "Afficher les choix", score: (c, a) => `${c} / ${a} bonnes réponses pour l'instant`, done: " — terminé. Ce que tu as raté\u202f: demande à ton binôme de review, puis sur le canal Teams."
    } : {
      title: "Check yourself", intro: "Answer each question in your head <em>before</em> revealing the options. Guessing from the options builds fluency; recalling builds storage.",
      show: "Show options", score: (c, a) => `${c} / ${a} correct so far`, done: " — done. Anything you missed: ask your review pair, then the Teams channel."
    };
    root.appendChild(el("h3", { text: t.title }));
    root.appendChild(el("p", { class: "aside", html: t.intro }));
    const score = el("p", { class: "score" });
    questions.forEach((q, i) => {
      const box = el("div", { class: "q" });
      box.appendChild(el("div", { class: "prompt", html: `<strong>${i + 1}.</strong> ${q.prompt}` }));
      const reveal = el("button", { class: "btn reveal", text: t.show });
      reveal.addEventListener("click", () => box.classList.add("revealed"));
      box.appendChild(reveal);
      const opts = el("div", { class: "options" });
      const order = q.options.map((_, j) => j).sort(() => Math.random() - 0.5); // the answer's position is no clue
      order.forEach(j => { const o = q.options[j];
        const b = el("button", { class: "opt", html: o });
        b.addEventListener("click", () => {
          if (box.classList.contains("answered")) return;
          box.classList.add("answered");
          state.answered++;
          if (j === q.answer) { b.classList.add("correct"); state.correct++; }
          else { b.classList.add("wrong"); opts.children[order.indexOf(q.answer)].classList.add("correct"); }
          score.textContent = t.score(state.correct, state.answered) + (state.answered === questions.length ? t.done : "");
        });
        opts.appendChild(b);
      });
      box.appendChild(opts);
      box.appendChild(el("div", { class: "explain", html: q.explain || "" }));
      root.appendChild(box);
    });
    root.appendChild(score);
  }
  return { render };
})();
