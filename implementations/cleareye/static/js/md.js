/* ClearEye — compact markdown renderer for memos & advisor analyses.
   Handles: #/##/### headings, **bold**, *em*, `code`, lists, tables,
   blockquotes, horizontal rules, paragraphs. No external deps. */
(function (global) {
  "use strict";

  function esc(s) {
    return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  }

  function inline(s) {
    return s
      .replace(/`([^`]+)`/g, function (_, c) { return "<code>" + c + "</code>"; })
      .replace(/\*\*\*([^*]+)\*\*\*/g, "<strong><em>$1</em></strong>")
      .replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>")
      .replace(/\*([^*\n]+)\*/g, "<em>$1</em>")
      .replace(/~~([^~]+)~~/g, "<s>$1</s>");
  }

  function isTableRow(line) {
    var t = line.trim();
    return t.indexOf("|") !== -1 && /^\|?.*\|.*\|?$/.test(t) && t.replace(/[|\s-:]/g, "").length >= 0 && t.split("|").length >= 3;
  }
  function isTableRule(line) {
    return /^\s*\|?[\s:|-]+\|?\s*$/.test(line) && line.indexOf("-") !== -1;
  }
  function splitRow(line) {
    var t = line.trim();
    if (t.charAt(0) === "|") t = t.slice(1);
    if (t.charAt(t.length - 1) === "|") t = t.slice(0, -1);
    return t.split("|").map(function (c) { return c.trim(); });
  }

  function render(md) {
    if (!md) return "";
    var lines = String(md).replace(/\r\n?/g, "\n").split("\n");
    var out = [];
    var i = 0, inUl = false, inOl = false, paraBuf = [];

    function closePara() {
      if (paraBuf.length) {
        out.push("<p>" + inline(esc(paraBuf.join(" "))) + "</p>");
        paraBuf = [];
      }
    }
    function closeLists() {
      if (inUl) { out.push("</ul>"); inUl = false; }
      if (inOl) { out.push("</ol>"); inOl = false; }
    }

    while (i < lines.length) {
      var raw = lines[i];
      var line = raw.trim();

      // blank
      if (!line) { closePara(); closeLists(); i++; continue; }

      // hr
      if (/^(-{3,}|_{3,}|\*{3,})$/.test(line)) {
        closePara(); closeLists(); out.push("<hr>"); i++; continue;
      }

      // heading
      var h = line.match(/^(#{1,6})\s+(.*)$/);
      if (h) {
        closePara(); closeLists();
        var lvl = Math.min(h[1].length, 3); // clamp: h1/h2 same visual tier, h3 italic
        out.push("<h" + lvl + ">" + inline(esc(h[2].replace(/#+\s*$/, ""))) + "</h" + lvl + ">");
        i++; continue;
      }

      // blockquote
      if (line.charAt(0) === ">") {
        closePara(); closeLists();
        var q = [];
        while (i < lines.length && lines[i].trim().charAt(0) === ">") {
          q.push(lines[i].trim().replace(/^>\s?/, ""));
          i++;
        }
        out.push("<blockquote>" + render(q.join("\n")) + "</blockquote>");
        continue;
      }

      // table
      if (isTableRow(line) && i + 1 < lines.length && isTableRule(lines[i + 1])) {
        closePara(); closeLists();
        var head = splitRow(line);
        i += 2;
        var body = [];
        while (i < lines.length && isTableRow(lines[i]) && lines[i].trim()) {
          if (!isTableRule(lines[i])) body.push(splitRow(lines[i]));
          i++;
        }
        var t = "<table><thead><tr>";
        head.forEach(function (c) { t += "<th>" + inline(esc(c)) + "</th>"; });
        t += "</tr></thead><tbody>";
        body.forEach(function (r) {
          t += "<tr>";
          for (var k = 0; k < head.length; k++) t += "<td>" + inline(esc(r[k] || "")) + "</td>";
          t += "</tr>";
        });
        t += "</tbody></table>";
        out.push(t);
        continue;
      }

      // unordered list
      if (/^[-*+]\s+/.test(line)) {
        closePara();
        if (inOl) { out.push("</ol>"); inOl = false; }
        if (!inUl) { out.push("<ul>"); inUl = true; }
        out.push("<li>" + inline(esc(line.replace(/^[-*+]\s+/, ""))) + "</li>");
        i++; continue;
      }

      // ordered list
      if (/^\d+[.)]\s+/.test(line)) {
        closePara();
        if (inUl) { out.push("</ul>"); inUl = false; }
        if (!inOl) { out.push("<ol>"); inOl = true; }
        out.push("<li>" + inline(esc(line.replace(/^\d+[.)]\s+/, ""))) + "</li>");
        i++; continue;
      }

      // paragraph text
      closeLists();
      paraBuf.push(line);
      i++;
    }
    closePara(); closeLists();
    return out.join("\n");
  }

  global.CEMarkdown = { render: render, escape: esc };
})(window);
