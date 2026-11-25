export function stepOf({ guess, running, result, revealed }) {
  if (revealed) return "explain";
  if (running || result) return "observe";
  return guess && guess.trim() ? "run" : "predict";
}

export function serviceStatus(containers) {
  const states = Object.entries(containers || {}).filter(([n]) => n.startsWith("lab-gap-svc")).map(([, s]) => s);
  if (!states.length) return "missing";
  return states.includes("running") ? "running" : "stopped";
}
