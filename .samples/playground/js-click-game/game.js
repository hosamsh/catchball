const startBtn = document.getElementById('start');
const arena = document.getElementById('arena');
const scoreEl = document.getElementById('score');
const timeEl = document.getElementById('time');
const statusEl = document.getElementById('status');

const R = 20;
const ROUND_SECONDS = 20;
const TARGET_SIZE = 48;

let score = 0;
let timeLeft = ROUND_SECONDS;
let timerId = null;
let running = false;
let targetEl = null;
let prevX = -1;
let prevY = -1;

function randInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}



function createTarget() {
  if (targetEl) return targetEl;
  targetEl = document.createElement('div');
  targetEl.className = 'target';
  targetEl.addEventListener('click', onTargetClick);
  targetEl.tabIndex = 0;
  arena.appendChild(targetEl);
  return targetEl;
}

function removeTarget() {
  if (!targetEl) return;
  targetEl.remove();
  targetEl = null;
}

function placeTarget() {
  const t = createTarget();
  const pad = 8;
  const w = t.offsetWidth || TARGET_SIZE;
  const h = t.offsetHeight || TARGET_SIZE;
  const maxX = Math.max(0, arena.clientWidth - w - pad);
  const maxY = Math.max(0, arena.clientHeight - h - pad);
  let x, y;
  const hasRangeX = maxX > pad;
  const hasRangeY = maxY > pad;
  const possibleX = hasRangeX ? (maxX - pad + 1) : 1;
  const possibleY = hasRangeY ? (maxY - pad + 1) : 1;

  // If there's only one possible coordinate pair, explicitly handle degenerate case.
  if (possibleX * possibleY <= 1) {
    x = hasRangeX ? pad : 0;
    y = hasRangeY ? pad : 0;
  } else {
    // Ensure we pick a coordinate that is different from previous.
    let attempts = 0;
    do {
      x = hasRangeX ? randInt(pad, maxX) : 0;
      y = hasRangeY ? randInt(pad, maxY) : 0;
      attempts++;
      // After many failed random attempts (very unlikely), pick a deterministic nearby spot.
      if (attempts > 50 && (x === prevX && y === prevY)) {
        if (hasRangeX) {
          x = (prevX === -1) ? randInt(pad, maxX) : (prevX < maxX ? prevX + 1 : pad);
        }
        if (hasRangeY && x === prevX && y === prevY) {
          y = (prevY === -1) ? randInt(pad, maxY) : (prevY < maxY ? prevY + 1 : pad);
        }
        break;
      }
    } while (x === prevX && y === prevY);
  }

  prevX = x;
  prevY = y;
  t.style.left = x + 'px';
  t.style.top = y + 'px';
  t.style.display = 'flex';
}

function endRound() {
  running = false;
  clearInterval(timerId);
  timerId = null;
  removeTarget();
  startBtn.disabled = false;
  statusEl.textContent = `Time's up! Final score: ${score}`;
}

function onTargetClick() {
  if (!running) return;
  score += 1;
  scoreEl.textContent = score;
  placeTarget();
}

function tick() {
  timeLeft -= 1;
  timeEl.textContent = timeLeft;
  if (timeLeft <= 0) endRound();
}

function startRound() {
  if (running) return;
  running = true;
  score = 0;
  timeLeft = ROUND_SECONDS;
  scoreEl.textContent = score;
  timeEl.textContent = timeLeft;
  statusEl.textContent = 'Go!';
  startBtn.disabled = true;
  placeTarget();
  clearInterval(timerId);
  timerId = setInterval(tick, 1000);
}


startBtn.addEventListener('click', startRound);
// keyboard enter
arena.addEventListener('keydown', (e)=>{ if(e.key==='Enter' && e.target.classList.contains('target')) e.target.click(); });

// initialize
scoreEl.textContent = 0;
timeEl.textContent = ROUND_SECONDS;
statusEl.textContent = 'Press Start to play';
startBtn.disabled = false;
removeTarget();
