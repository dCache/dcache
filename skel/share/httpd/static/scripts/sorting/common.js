
/**
 * Common Javascript code which applies to all static HTML files.
 *
 * Copyright (C) 2025 Deutsches Elektronen-Synchrotron
 * Tino Reichardt <tino.reichardt@desy.de>
 */

let isHumanReadable = true;
let StrToBeSearched = "table tbody td.total, table tbody td.free, table tbody td.precious";

function BytesToHumanReadable(bytes) {
  const units = ['MiB', 'GiB', 'TiB', 'PiB', 'EiB'];
  let value = bytes, i = 0;
  while (value >= 1024 && i < units.length - 1) {
    value /= 1024;
    i++;
  }
  return `${value.toFixed(1)} ${units[i]}`;
}

function ClickToggleButton(btn) {
  btn.textContent = isHumanReadable ? ' Switch to MiB ' : 'Switch to human readable';
  const totalCells = document.querySelectorAll(StrToBeSearched);
  totalCells.forEach((cell) => {
    const raw = parseInt(cell.getAttribute('data-st-key'), 10);
    cell.textContent = isHumanReadable ? BytesToHumanReadable(raw) : raw;
  });
  isHumanReadable = !isHumanReadable;
}

// when fully loaded - change some static content
document.addEventListener("DOMContentLoaded", () => {
  // fix sorting the times at: http://host:2288/cellInfo
  const ping = document.querySelector("th.ping");
  if (ping) { ping.classList.add("sorttable_numeric"); }

  const main = document.getElementById("main");
  if (main) {
    // check if we can modify some raw values for human readability
    const totalCells = document.querySelectorAll(StrToBeSearched);
    if (totalCells.length <= 0) { return; }
    totalCells.forEach((cell) => {
      if (!cell.hasAttribute('data-st-key')) {
        // remember raw value for later use
        const rawValue = parseInt(cell.textContent.replace(/[^\d]/g, ''), 10);
        if (!isNaN(rawValue)) { cell.setAttribute('data-st-key', rawValue); }
      }
    });

    const toggleBtn = document.createElement("button");
    main.parentNode.insertBefore(toggleBtn, main);
    toggleBtn.addEventListener("click", () => ClickToggleButton(toggleBtn));
    toggleBtn.click();
  }
});
