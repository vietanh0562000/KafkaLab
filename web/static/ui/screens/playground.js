import { html } from "../shell.js";
import { Workspace } from "../workspace.js";

export const Playground = ({ lab }) => html`<${Workspace} lab=${lab} />`;
