import { render } from "preact";
import { useEffect, useState } from "preact/hooks";
import { html, Header, Footer, Safe, TABS } from "./shell.js";
import { useLab } from "./store.js";
import { Playground } from "./screens/playground.js";
import { Lessons } from "./screens/lessons.js";
import { Scenarios } from "./screens/scenarios.js";
import { Dashboard } from "./screens/dashboard.js";

const SCREENS = { playground: Playground, lessons: Lessons, scenarios: Scenarios, dashboard: Dashboard };

const routeOf = () => {
  const h = location.hash.slice(1).split("/")[0];
  return TABS.some(([id]) => id === h) ? h : "playground";
};

function App() {
  const [route, setRoute] = useState(routeOf);
  const lab = useLab();
  useEffect(() => {
    const on = () => setRoute(routeOf());
    addEventListener("hashchange", on);
    return () => removeEventListener("hashchange", on);
  }, []);
  return html`
    <div class="app">
      <${Header} route=${route} lab=${lab} />
      <main class="page"><${Safe} name=${route} key=${route}>
        <${SCREENS[route]} lab=${lab} />
      <//></main>
      <${Footer} lab=${lab} />
      ${lab.toast && html`<div class=${"toast" + (lab.toast.err ? " err" : "")}>${lab.toast.text}</div>`}
    </div>`;
}

render(html`<${App} />`, document.getElementById("app"));
