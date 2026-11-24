import { useEffect, useRef, useState } from "preact/hooks";
import { api, safeStore } from "./api.js";
import { mergeLastGood } from "./logic/merge.js";

export const LESSON_TOPIC = "orders";
export const SERVICE_TOPIC = "lab.short-term";
export const isScenarioTopic = (name) => name.startsWith("lab.") || name.startsWith("dlq-lab.");

const POLL_MS = 700;

export function useLab() {
  const [st, setSt] = useState(null);
  const [snapshot, setSnapshot] = useState(null);
  const [live, setLive] = useState(false);
  const [frozen, setFrozenState] = useState(false);
  const [topic, setTopic] = useState(() => {
    const t = safeStore.get("pg-topic", LESSON_TOPIC);
    return isScenarioTopic(t) ? LESSON_TOPIC : t;
  });
  const [log, setLog] = useState([]);
  const [cli, setCli] = useState("");
  const [toast, setToast] = useState(null);
  const [stepsByScenario, setSteps] = useState({});
  const [resultsByScenario, setResults] = useState({});

  const cache = useRef({});
  const topicRef = useRef(topic);
  topicRef.current = topic;
  const frozenRef = useRef(false);
  const latest = useRef({ st: null, snapshot: null });
  const pokeRef = useRef(() => {});

  const note = (text, err = false) => {
    setLog((l) => [{ t: new Date().toLocaleTimeString("vi-VN"), text, err }, ...l].slice(0, 80));
    setToast({ text, err });
  };

  useEffect(() => {
    if (!toast) return undefined;
    const id = setTimeout(() => setToast(null), 3500);
    return () => clearTimeout(id);
  }, [toast]);

  // act runs one user action, logs it and shows the equivalent CLI command the server sent back;
  // describe, when given, turns the response into the log line.
  const act = async (label, method, path, body, describe) => {
    try {
      const res = await api(method, path, body);
      if (res.cli) setCli(res.cli);
      note(describe ? describe(res) : label);
      pokeRef.current();
      return res;
    } catch (e) {
      note(`${label}: ${e.message}`, true);
      return null;
    }
  };

  const setFrozen = (on) => {
    frozenRef.current = on;
    setFrozenState(on);
    if (!on) {
      setSt(latest.current.st);
      setSnapshot(latest.current.snapshot);
    }
  };

  useEffect(() => {
    let stop = false, timer;
    const tick = async () => {
      clearTimeout(timer);
      const asked = topicRef.current;
      try {
        const raw = await api("GET", `/api/pg/state?topic=${encodeURIComponent(asked)}`);
        if (stop) return;
        // An answer for a topic the user has already left must not repaint the map.
        if (asked === topicRef.current) {
          const s = mergeLastGood(raw, cache.current, asked);
          latest.current.st = s;
          if (!frozenRef.current) setSt(s);
        }
        setLive(true);
      } catch {
        if (!stop) setLive(false);
      }
      if (!stop) timer = setTimeout(tick, POLL_MS);
    };
    pokeRef.current = tick;
    tick();

    const es = new EventSource("/api/events");
    const data = (e) => JSON.parse(e.data).data;
    es.addEventListener("snapshot", (e) => {
      const s = data(e);
      latest.current.snapshot = s;
      if (!frozenRef.current) setSnapshot(s);
    });
    es.addEventListener("log", (e) => {
      const ev = JSON.parse(e.data);
      setLog((l) => [{ t: new Date(ev.at || Date.now()).toLocaleTimeString("vi-VN"), text: ev.data, err: false }, ...l].slice(0, 80));
    });
    es.addEventListener("step", (e) => {
      const ev = data(e);
      setSteps((all) => {
        const list = [...(all[ev.scenario] || [])];
        const i = list.findIndex((x) => x.step === ev.step);
        if (i >= 0) list[i] = { ...list[i], ...ev }; else list.push(ev);
        return { ...all, [ev.scenario]: list };
      });
    });
    es.addEventListener("scenario", (e) => {
      const res = data(e);
      if (res.id !== "reset") setResults((all) => ({ ...all, [res.id]: res }));
    });
    api("GET", "/api/snapshot").then((s) => {
      if (stop || latest.current.snapshot) return;
      latest.current.snapshot = s;
      if (!frozenRef.current) setSnapshot(s);
    }).catch(() => {});
    return () => { stop = true; clearTimeout(timer); es.close(); };
  }, []);

  // A scenario topic is a one-off look; it is not where the playground reopens.
  useEffect(() => {
    if (!isScenarioTopic(topic)) safeStore.set("pg-topic", topic);
    pokeRef.current();
  }, [topic]);

  // Fall back to an existing playground topic once the list is known; none means the empty state.
  const names = st && st.topics ? st.topics.map((t) => t.name) : null;
  useEffect(() => {
    if (!names || frozenRef.current) return;
    if (topic && names.includes(topic)) return;
    const own = names.filter((n) => !isScenarioTopic(n));
    setTopic(own.includes(LESSON_TOPIC) ? LESSON_TOPIC : own[0] || "");
  }, [names && names.join(",")]);

  const clearScenario = (id) => {
    setSteps((all) => ({ ...all, [id]: [] }));
    setResults((all) => { const next = { ...all }; delete next[id]; return next; });
  };

  return { st, snapshot, live, frozen, setFrozen, topic, setTopic, act, note, log, cli, toast,
    stepsByScenario, resultsByScenario, clearScenario };
}
