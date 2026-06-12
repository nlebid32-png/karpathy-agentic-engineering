/* ClearEye — report hydration.
   Reads window.CE_PREFILL (server-injected job JSON) and typesets the
   committee memorandum: stamp, conviction gauge, abstract, dossiers,
   chairman's memo, exhibits. */
(function () {
  "use strict";

  var $ = function (sel, root) { return (root || document).querySelector(sel); };
  var md = window.CEMarkdown;
  var job = window.CE_PREFILL || {};
  var deal = job.deal || {};
  var memo = job.memo || "";

  /* ---------------- helpers ---------------- */
  function fmtMoney(n) {
    if (n == null || n === "" || isNaN(Number(n))) return null;
    n = Number(n);
    if (n >= 1e6) return "$" + (n / 1e6).toFixed(n % 1e6 === 0 ? 0 : 1) + "M";
    return "$" + n.toLocaleString("en-US");
  }
  function fmtMoneyFull(n) {
    if (n == null || n === "" || isNaN(Number(n))) return null;
    return "$" + Number(n).toLocaleString("en-US");
  }
  function pct(n) {
    if (n == null || n === "" || isNaN(Number(n))) return null;
    return Number(n).toFixed(1).replace(/\.0$/, "") + "%";
  }
  function titleCase(s) {
    return String(s || "").replace(/\b\w/g, function (c) { return c.toUpperCase(); });
  }

  /* ---------------- verdict ---------------- */
  function deriveVerdict() {
    var v = String(job.verdict || "").toUpperCase().trim();
    if (/NO[\s-]?GO|PASS|REJECT|DECLINE/.test(v)) return "NO-GO";
    if (/CONDITIONAL|CAUTION|QUALIFIED/.test(v)) return "CONDITIONAL";
    if (/^GO\b|PROCEED|APPROVE/.test(v)) return "GO";
    // fall back to the memo text
    if (/KILL\s+SHOT|NO[\s-]?GO|\bPASS\b|REJECT/i.test(memo)) return "NO-GO";
    if (/CONDITIONAL|PROCEED\s+WITH\s+CAUTION/i.test(memo)) return "CONDITIONAL";
    if (/\bGO\b|PROCEED/i.test(memo)) return "GO";
    return "CONDITIONAL";
  }

  function deriveConfidence() {
    var c = Number(job.confidence);
    if (c >= 1 && c <= 100) return Math.round(c);
    var m = memo.match(/(?:confidence|conviction)[:\s]*(\d{1,3})\s*%/i) ||
            memo.match(/(\d{1,3})\s*%\s*(?:confidence|conviction)/i);
    if (m) { c = Number(m[1]); if (c >= 1 && c <= 100) return c; }
    return 72;
  }

  /* Extract the kill-shot / thesis paragraph for the hero */
  function deriveKillshot() {
    var m = memo.match(/#{1,3}\s*[^\n]*(?:KILL SHOT|VERDICT|RECOMMENDATION|EXECUTIVE SUMMARY)[^\n]*\n+([\s\S]*?)(?=\n#{1,3}\s|\n---|$)/i);
    var txt = m ? m[1] : memo.split(/\n#{1,3}\s/)[0];
    txt = (txt || "").replace(/^[-=\s]+/, "").split(/\n{2,}/)[0] || "";
    txt = txt.replace(/\*\*/g, "").replace(/[#>*_`]/g, "").trim();
    if (txt.length > 540) txt = txt.slice(0, 540).replace(/\s+\S*$/, "") + " …";
    return txt;
  }

  var verdict = deriveVerdict();
  var conf = deriveConfidence();

  /* ---------------- hero ---------------- */
  var stampEl = $("#stamp");
  stampEl.textContent = verdict;
  stampEl.classList.add(
    verdict === "GO" ? "stamp--go" : verdict === "CONDITIONAL" ? "stamp--conditional" : "stamp--nogo"
  );

  var killshotWrap = $("#killshot");
  if (verdict === "GO") killshotWrap.classList.add("is-go");
  if (verdict === "CONDITIONAL") killshotWrap.classList.add("is-conditional");
  $("#killshot-label").textContent =
    verdict === "GO" ? "The Committee's View" :
    verdict === "CONDITIONAL" ? "The Committee's Reservations" : "The Kill Shot";
  $("#killshot-text").textContent = deriveKillshot() ||
    "The committee has filed its memorandum below.";

  // deal name + meta
  $("#deal-name").textContent = deal.deal_name || "Untitled Deal";
  var metaBits = [];
  if (deal.market) metaBits.push(deal.market);
  if (deal.property_type) metaBits.push(titleCase(deal.property_type));
  if (deal.units_or_sqft) metaBits.push(deal.units_or_sqft);
  $("#deal-meta").innerHTML = metaBits.map(function (b) {
    return "<span>" + md.escape(String(b)) + "</span>";
  }).join('<span style="color:var(--brass-soft)">·</span>');

  // gauge
  var arc = $("#gauge-arc");
  var color = verdict === "GO" ? "var(--green)" : verdict === "CONDITIONAL" ? "var(--amber)" : "var(--oxblood)";
  arc.style.stroke = color;
  var C = 2 * Math.PI * 64; // r=64
  var half = C / 2;
  arc.setAttribute("stroke-dasharray", half + " " + C);
  arc.setAttribute("stroke-dashoffset", String(half));
  $("#gauge-pct").textContent = conf + "%";
  requestAnimationFrame(function () {
    setTimeout(function () {
      arc.setAttribute("stroke-dashoffset", String(half * (1 - conf / 100)));
    }, 350);
  });

  // key figures
  var figs = [];
  if (deal.asking_price != null) figs.push([fmtMoneyFull(deal.asking_price), "Asking Price"]);
  if (deal.cap_rate != null) figs.push([pct(deal.cap_rate), "Going-In Cap"]);
  if (deal.projected_irr != null) figs.push([pct(deal.projected_irr) + ' <span class="delta-down">†</span>', "Sponsor IRR"]);
  if (deal.equity_multiple != null) figs.push([deal.equity_multiple + "x", "Equity Multiple"]);
  if (figs.length < 4 && deal.projected_noi != null) figs.push([fmtMoneyFull(deal.projected_noi), "Year-1 NOI"]);
  if (figs.length < 4 && deal.occupancy_rate != null) figs.push([pct(deal.occupancy_rate), "Occupancy"]);
  $("#keyfigs-inner").innerHTML = figs.slice(0, 4).map(function (f) {
    return '<div class="keyfig"><b>' + f[0] + "</b><span>" + f[1] + "</span></div>";
  }).join("");

  /* ---------------- §1 abstract ---------------- */
  var ROWS = [
    ["deal_name", "Deal"], ["property_type", "Asset Class"], ["market", "Market"],
    ["address", "Address"], ["units_or_sqft", "Scale"], ["year_built", "Vintage"],
    ["asking_price", "Asking Price", fmtMoneyFull], ["projected_noi", "Projected NOI (Yr 1)", fmtMoneyFull],
    ["cap_rate", "Going-In Cap Rate", pct], ["exit_cap_rate", "Exit Cap Rate", pct],
    ["projected_irr", "Projected IRR", pct], ["equity_multiple", "Equity Multiple", function (v) { return v + "x"; }],
    ["occupancy_rate", "Occupancy", pct], ["rent_growth_assumption", "Rent Growth Assumed", pct],
    ["expense_ratio", "Expense Ratio", pct], ["hold_period_years", "Hold Period", function (v) { return v + " years"; }],
    ["debt_terms", "Debt Terms"], ["exit_strategy", "Exit Strategy"],
    ["sponsor_name", "Sponsor"], ["sponsor_track_record", "Stated Track Record"]
  ];
  var colA = [], colB = [];
  var visible = ROWS.filter(function (r) {
    var v = deal[r[0]];
    return !(v == null || v === "");
  });
  visible.forEach(function (r, i) {
    var v = deal[r[0]];
    var fmt = r[2];
    var shown = fmt ? (fmt(v) == null ? String(v) : fmt(v)) : String(v);
    var row =
      '<div class="fig-row"><dt>' + r[1] + '</dt><span class="leader"></span><dd>' +
      md.escape(shown) + "</dd></div>";
    (i < Math.ceil(visible.length / 2) ? colA : colB).push(row);
  });
  $("#abstract-a").innerHTML = colA.join("");
  $("#abstract-b").innerHTML = colB.join("");

  var missing = deal.missing_data;
  if (missing && missing.length) {
    var list = Array.isArray(missing) ? missing : [String(missing)];
    $("#missing-data").innerHTML =
      '<span class="smallcaps" style="color:var(--oxblood)">Not disclosed by sponsor</span> — ' +
      list.map(function (x) { return md.escape(String(x)); }).join("; ");
    $("#missing-data").style.display = "";
  }

  /* ---------------- §2 the council ---------------- */
  var ADV_META = {
    "Bear Case Analyst":         { roman: "Advisor I",   css: "var(--adv-bear)",   brief: "Argues the case against — every deal, no exceptions." },
    "Cognitive Bias Detector":   { roman: "Advisor II",  css: "var(--adv-bias)",   brief: "Reads the sponsor's language for motivated reasoning." },
    "Market Validation Auditor": { roman: "Advisor III", css: "var(--adv-market)", brief: "Checks every claim against observable market data." },
    "Tax & Structure Optimizer": { roman: "Advisor IV",  css: "var(--adv-tax)",    brief: "Inspects the waterfall, the debt, and who really gets paid." },
    "Exit Strategy Specialist":  { roman: "Advisor V",   css: "var(--adv-exit)",   brief: "Asks the only question that matters: who buys it from you?" }
  };
  var ORDER = ["Bear Case Analyst", "Cognitive Bias Detector", "Market Validation Auditor", "Tax & Structure Optimizer", "Exit Strategy Specialist"];

  function advisorText(val) {
    if (val == null) return "";
    if (typeof val === "string") return val;
    return val.analysis || val.text || JSON.stringify(val);
  }
  function advisorScore(text) {
    var m = String(text).match(/(?:score|rating)[:\s]*\**\s*(\d{1,3})\s*\/\s*100/i);
    return m ? Math.min(100, Number(m[1])) : null;
  }
  function firstLine(text) {
    var t = String(text).replace(/^#+[^\n]*\n+/, "").replace(/[#>*_`|]/g, " ").replace(/\s+/g, " ").trim();
    return t.slice(0, 150);
  }

  var advisors = job.advisors || {};
  var names = ORDER.filter(function (n) { return advisors[n]; });
  // include any unknown advisors appended
  Object.keys(advisors).forEach(function (n) { if (names.indexOf(n) === -1) names.push(n); });

  var dossierHtml = names.map(function (name, i) {
    var meta = ADV_META[name] || { roman: "Advisor " + (i + 1), css: "var(--brass)", brief: "" };
    var text = advisorText(advisors[name]);
    var score = advisorScore(text);
    var sealHtml = score != null
      ? '<div class="seal" style="--seal-color:' + meta.css + '"><b>' + score + "</b><span>/ 100</span></div>"
      : "";
    return (
      '<details class="dossier rise" style="--adv:' + meta.css + ';--d:' + (i * 90) + 'ms">' +
      "<summary>" + sealHtml +
      '<div class="dossier-id"><div class="dossier-roman">' + meta.roman + "</div>" +
      '<div class="dossier-name">' + md.escape(name) + "</div>" +
      '<div class="dossier-take">' + md.escape(firstLine(text)) + "…</div></div>" +
      '<span class="open-cue"><span class="closed-label">Read analysis ↓</span><span class="open-label">Fold away ↑</span></span>' +
      "</summary>" +
      '<div class="dossier-body"><div class="typeset">' + md.render(text) + "</div></div>" +
      "</details>"
    );
  }).join("");
  $("#dossiers").innerHTML = dossierHtml || '<p class="section-lede">No advisor analyses on file.</p>';

  /* ---------------- §3 chairman's memorandum ---------------- */
  $("#memo-body").innerHTML = md.render(memo);

  /* ---------------- §4 exhibits ---------------- */
  function exhibit(id, content, pre) {
    var el = $(id);
    if (!el) return;
    if (!content || !String(content).trim()) { el.closest(".exhibit").style.display = "none"; return; }
    el.innerHTML = pre
      ? "<pre>" + md.escape(String(content)) + "</pre>"
      : '<div class="typeset">' + md.render(String(content)) + "</div>";
  }
  exhibit("#ex-stress", job.stress_table, true);
  exhibit("#ex-validation", job.validation_report);
  exhibit("#ex-bias", job.bias_report);
  exhibit("#ex-premortem", job.premortem_report);
  exhibit("#ex-macro", job.macro_brief);

  /* ---------------- folio & actions ---------------- */
  var when = job.generated_at ? new Date(job.generated_at) : new Date();
  var dateStr = when.toLocaleDateString("en-US", { year: "numeric", month: "long", day: "numeric" });
  Array.prototype.forEach.call(document.querySelectorAll(".js-date"), function (el) { el.textContent = dateStr; });

  var copyBtn = $("#copy-link");
  if (copyBtn) copyBtn.addEventListener("click", function () {
    navigator.clipboard.writeText(location.href).then(function () {
      copyBtn.textContent = "LINK COPIED ✓";
      setTimeout(function () { copyBtn.textContent = "COPY SHARE LINK"; }, 2200);
    });
  });
  var printBtn = $("#print-btn");
  if (printBtn) printBtn.addEventListener("click", function () { window.print(); });

  /* toc highlight */
  var tocLinks = Array.prototype.slice.call(document.querySelectorAll(".memo-toc a[href^='#']"));
  if ("IntersectionObserver" in window) {
    var byId = {};
    tocLinks.forEach(function (a) { byId[a.getAttribute("href").slice(1)] = a; });
    var io = new IntersectionObserver(function (entries) {
      entries.forEach(function (e) {
        if (e.isIntersecting) {
          tocLinks.forEach(function (a) { a.classList.remove("is-here"); });
          var a = byId[e.target.id];
          if (a) a.classList.add("is-here");
        }
      });
    }, { rootMargin: "-20% 0px -70% 0px" });
    ["s-abstract", "s-council", "s-memo", "s-exhibits"].forEach(function (id) {
      var el = document.getElementById(id);
      if (el) io.observe(el);
    });
  }
})();
