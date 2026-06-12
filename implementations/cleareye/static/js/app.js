/* ClearEye — intake & committee-session theater.
   Submits the OM, narrates the pipeline via timed minutes + real SSE
   checkpoints, then hands off to /report/<job_id>. */
(function () {
  "use strict";

  var $ = function (sel, root) { return (root || document).querySelector(sel); };
  var $$ = function (sel, root) { return Array.prototype.slice.call((root || document).querySelectorAll(sel)); };

  /* ---------- Intake tabs ---------- */
  var tabs = $$(".intake-tab");
  var panes = { paste: $("#pane-paste"), upload: $("#pane-upload"), url: $("#pane-url") };
  tabs.forEach(function (tab) {
    tab.addEventListener("click", function () {
      tabs.forEach(function (t) { t.setAttribute("aria-selected", "false"); });
      tab.setAttribute("aria-selected", "true");
      Object.keys(panes).forEach(function (k) {
        if (panes[k]) panes[k].style.display = (k === tab.dataset.pane) ? "" : "none";
      });
    });
  });

  var ta = $("#om-text");
  var errBox = $("#intake-error");

  function showError(html) {
    errBox.innerHTML = html;
    errBox.style.display = "";
    errBox.scrollIntoView({ behavior: "smooth", block: "nearest" });
  }
  function clearError() { errBox.style.display = "none"; errBox.innerHTML = ""; }

  /* ---------- Specimen deal ---------- */
  var SPECIMEN = [
    "OFFERING MEMORANDUM — SUNSET RIDGE APARTMENTS",
    "124-unit multifamily value-add | Phoenix, AZ (Tempe submarket)",
    "Asking Price: $18,500,000 | Going-In Cap Rate: 5.2%",
    "Year Built: 1987 | Current Occupancy: 94%",
    "Projected Year 1 NOI: $962,000",
    "Rent Growth Assumption: 4.5% annually (market avg: 3.1%)",
    "Expense Ratio: 42% of EGI",
    "Sponsor: Apex Capital Partners — 15 years experience, 2,400 units acquired",
    "Track Record: 22% average IRR across 14 realized deals (cherry-picked per footnote 3)",
    "Investment Thesis: Light value-add through unit renovations at $8,500/unit",
    "achieving $175/month rent premium. 5-year hold target.",
    "Projected IRR: 18.2% | Equity Multiple: 2.4x",
    "Exit Cap Rate: 5.0% (assumes continued cap rate compression)",
    "Debt: 70% LTV, 5.5% fixed rate, 5-year term interest-only",
    "Minimum Investment: $250,000 | Total Equity Raise: $5,550,000",
    '"Given strong institutional demand for Phoenix multifamily and our proven',
    'renovation playbook, we are highly confident in achieving projected returns."'
  ].join("\n");

  var specBtn = $("#load-specimen");
  if (specBtn) specBtn.addEventListener("click", function () {
    ta.value = SPECIMEN;
    ta.focus();
    clearError();
  });
  if (/[?&]specimen=1/.test(location.search) && ta) ta.value = SPECIMEN;

  /* ---------- PDF upload ---------- */
  var drop = $("#drop-zone");
  var fileInput = $("#pdf-input");
  if (drop && fileInput) {
    drop.addEventListener("click", function () { fileInput.click(); });
    ["dragover", "dragenter"].forEach(function (ev) {
      drop.addEventListener(ev, function (e) { e.preventDefault(); drop.classList.add("dragover"); });
    });
    ["dragleave", "drop"].forEach(function (ev) {
      drop.addEventListener(ev, function (e) { e.preventDefault(); drop.classList.remove("dragover"); });
    });
    drop.addEventListener("drop", function (e) {
      if (e.dataTransfer.files.length) handlePdf(e.dataTransfer.files[0]);
    });
    fileInput.addEventListener("change", function () {
      if (fileInput.files.length) handlePdf(fileInput.files[0]);
    });
  }
  function handlePdf(file) {
    clearError();
    drop.textContent = "Extracting — " + file.name + " …";
    var fd = new FormData();
    fd.append("pdf", file);
    fetch("/upload", { method: "POST", body: fd })
      .then(function (r) { return r.json().then(function (j) { return { ok: r.ok, j: j }; }); })
      .then(function (res) {
        if (!res.ok || res.j.error) throw new Error(res.j.error || "Extraction failed");
        ta.value = res.j.text;
        drop.textContent = "Extracted " + res.j.pages + " pages from " + res.j.filename + " — review left tab.";
        tabs[0].click();
      })
      .catch(function (e) {
        drop.textContent = "Drop the offering memorandum PDF here — or click to choose";
        showError("PDF intake failed: " + e.message + ". Paste the text instead.");
      });
  }

  /* ---------- Listing URL ---------- */
  var urlBtn = $("#fetch-url-btn");
  if (urlBtn) urlBtn.addEventListener("click", function () {
    var url = $("#listing-url").value.trim();
    if (!url) return;
    clearError();
    urlBtn.disabled = true; urlBtn.textContent = "FETCHING…";
    fetch("/api/fetch-url", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ url: url })
    })
      .then(function (r) { return r.json(); })
      .then(function (j) {
        if (j.om_text) { ta.value = j.om_text; tabs[0].click(); }
        else throw new Error(j.error || "Could not extract listing");
      })
      .catch(function (e) { showError("Listing fetch failed: " + e.message); })
      .finally(function () { urlBtn.disabled = false; urlBtn.textContent = "FETCH LISTING"; });
  });

  /* ---------- Quota note ---------- */
  fetch("/api/usage").then(function (r) { return r.json(); }).then(function (q) {
    var el = $("#quota-note");
    if (!el || !q || q.limit == null) return;
    var left = Math.max(0, (q.limit || 0) - (q.used || 0));
    el.innerHTML = "Reviews remaining this month: <b>" + left + "</b> of " + q.limit +
      (q.tier === "free" ? " · complimentary tier" : "");
  }).catch(function () {});

  /* =====================================================================
     THE SESSION — committee theater
     ===================================================================== */
  var ADVISORS = [
    { key: "bear",   name: "Bear Case Analyst",        cssVar: "var(--adv-bear)" },
    { key: "bias",   name: "Cognitive Bias Detector",  cssVar: "var(--adv-bias)" },
    { key: "market", name: "Market Validation Auditor",cssVar: "var(--adv-market)" },
    { key: "tax",    name: "Tax & Structure Optimizer",cssVar: "var(--adv-tax)" },
    { key: "exit",   name: "Exit Strategy Specialist", cssVar: "var(--adv-exit)" }
  ];

  // Time-scripted minutes (seconds → entry). Real SSE events gate the finale.
  var SCRIPT = [
    [0,   "Session opened. Offering memorandum entered into the record.", true],
    [2,   "Intake desk — parsing deal structure, figures, and sponsor claims…"],
    [16,  "Deal abstract drafted. Key figures extracted."],
    [18,  "Stress desk — running the sensitivity grid across rent, cap, and rate scenarios…"],
    [26,  "Audit desk — validating every sponsor assumption against market benchmarks…"],
    [33,  "Macro desk — pulling rate environment and supply pipeline for the subject market…"],
    [40,  "Pre-mortem — simulating the ways this deal fails before it begins…"],
    [50,  "The council convenes. Five advisors deliberating independently.", true],
    [52,  null, false, "seatsLive"],
    [68,  "Analyses returned. Anonymizing for blind peer review…"],
    [76,  "Blind peer review — each advisor critiques the others' work, unsigned."],
    [84,  "The Chairman has the floor. Drafting the committee memorandum…", true],
    [92,  "Weighing dissents and synthesizing the verdict…"]
  ];

  var theater = $("#theater");
  var intake = $("#intake-section");
  var minutesEl = $("#minutes");
  var clockEl = $("#session-clock");
  var fillEl = $("#progress-fill");
  var stageEl = $("#progress-stage");
  var submitBtn = $("#submit-btn");

  var t0 = null, timerId = null, scriptIdx = 0, done = false;

  function pad(n) { return (n < 10 ? "0" : "") + n; }
  function clock() {
    var s = Math.floor((Date.now() - t0) / 1000);
    return pad(Math.floor(s / 60)) + ":" + pad(s % 60);
  }

  function addMinute(text, major, isError) {
    var div = document.createElement("div");
    div.className = "minutes-line" + (major ? " is-major" : "") + (isError ? " is-error" : "");
    div.innerHTML = '<span class="ts">' + clock() + "</span><span class='tx'></span>";
    div.querySelector(".tx").textContent = text;
    minutesEl.appendChild(div);
    minutesEl.scrollTop = minutesEl.scrollHeight;
  }

  function seatsLive() {
    $$(".seat").forEach(function (seat, i) {
      setTimeout(function () {
        seat.classList.add("is-live");
        seat.querySelector(".seat-state").textContent = "Deliberating";
        addMinute("The " + ADVISORS[i].name + " has taken the floor.");
      }, i * 1700);
      // advisors "finish" staggered later in the council window
      setTimeout(function () {
        if (done) return;
        seat.classList.remove("is-live");
        seat.classList.add("is-done");
        seat.querySelector(".seat-state").textContent = "Analysis filed";
      }, 14000 + i * 2600);
    });
  }

  function tick() {
    var elapsed = (Date.now() - t0) / 1000;
    clockEl.textContent = "IN SESSION — " + clock();

    while (scriptIdx < SCRIPT.length && SCRIPT[scriptIdx][0] <= elapsed) {
      var entry = SCRIPT[scriptIdx];
      if (entry[3] === "seatsLive") seatsLive();
      else if (entry[1]) addMinute(entry[1], !!entry[2]);
      scriptIdx++;
    }
    // progress: glide toward 92%, finish only on real done
    var pct = Math.min(92, Math.round((elapsed / 105) * 100));
    if (!done) fillEl.style.width = pct + "%";
  }

  function startTheater() {
    intake.style.display = "none";
    theater.classList.add("is-active");
    window.scrollTo({ top: 0, behavior: "smooth" });
    t0 = Date.now();
    timerId = setInterval(tick, 500);
    tick();
  }

  function finish(jobId) {
    if (done) return;
    done = true;
    clearInterval(timerId);
    fillEl.style.width = "100%";
    stageEl.textContent = "VERDICT REACHED";
    $$(".seat").forEach(function (seat) {
      seat.classList.remove("is-live");
      seat.classList.add("is-done");
      seat.querySelector(".seat-state").textContent = "Analysis filed";
    });
    addMinute("The committee has reached a verdict. Preparing the memorandum…", true);
    setTimeout(function () { location.href = "/report/" + jobId; }, 1400);
  }

  function fail(msg) {
    if (done) return;
    done = true;
    clearInterval(timerId);
    addMinute("Session adjourned with an error: " + msg, true, true);
    stageEl.textContent = "SESSION HALTED";
    var back = document.createElement("div");
    back.style.marginTop = "18px";
    back.innerHTML = '<button class="btn" id="retry-btn">RETURN TO INTAKE</button>';
    $("#theater .doc").appendChild(back);
    $("#retry-btn").addEventListener("click", function () { location.reload(); });
  }

  /* ---------- Submit ---------- */
  if (submitBtn) submitBtn.addEventListener("click", function () {
    var text = (ta.value || "").trim();
    clearError();
    if (text.length < 80) {
      showError("The committee needs more to work with — paste the offering memorandum text (at least a few lines of deal terms).");
      return;
    }
    submitBtn.disabled = true; submitBtn.textContent = "CONVENING…";

    fetch("/analyze", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ om_text: text })
    })
      .then(function (r) { return r.json().then(function (j) { return { status: r.status, j: j }; }); })
      .then(function (res) {
        if (res.status === 429 || (res.j && res.j.quota_exceeded)) {
          submitBtn.disabled = false; submitBtn.textContent = "SUBMIT FOR REVIEW";
          showError(
            "Monthly allowance exhausted — " + (res.j.error || "") +
            ' <a href="/pricing">Review the fee schedule →</a>'
          );
          return;
        }
        if (!res.j.job_id) throw new Error(res.j.error || "Could not open a session");
        startTheater();
        watch(res.j.job_id);
      })
      .catch(function (e) {
        submitBtn.disabled = false; submitBtn.textContent = "SUBMIT FOR REVIEW";
        showError("Could not convene the committee: " + e.message);
      });
  });

  /* ---------- Watch: SSE with polling fallback ---------- */
  function watch(jobId) {
    var pollId = null;
    function poll() {
      fetch("/status/" + jobId)
        .then(function (r) { return r.json(); })
        .then(function (j) {
          if (j.status === "done") { clearInterval(pollId); finish(jobId); }
          else if (j.status === "error") { clearInterval(pollId); fail(j.message || "pipeline error"); }
        })
        .catch(function () {});
    }
    pollId = setInterval(poll, 4000);

    if (window.EventSource) {
      try {
        var es = new EventSource("/stream/" + jobId);
        es.addEventListener("done", function () { es.close(); clearInterval(pollId); finish(jobId); });
        es.addEventListener("error", function (e) {
          if (e.data) { es.close(); clearInterval(pollId); fail(e.data); }
        });
        es.addEventListener("close", function () { es.close(); });
      } catch (err) { /* polling covers us */ }
    }
  }
})();
