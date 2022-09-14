import { API_CONFIG } from "../../config/api";
import { Node, Tag } from "./catalog.slice";

export const fetchNodes = async (): Promise<Node[]> => {
  let response = await fetch(`${API_CONFIG.origin}/api/v1/catalog`);
  let data = await response.json();
  console.log(data)
  
  return []
};

export const isAuthoritative = (node: Node) =>
  node.url.startsWith(window.location.origin);
