import { Node, Tag } from "./catalog.slice";

export const fetchNodes = async (): Promise<Node[]> => {
  const hosts = [
  ];

  const responses = await Promise.all(
    hosts.map((i) =>
      fetch(`${i}/api/node`)
        .then((res) => res.json() as Promise<Node>)
        .catch((i) => null)
    )
  );

  return responses.filter((i) => i) as Node[];
};

export const isAuthoritative = (node: Node) =>
  node.url.startsWith(window.location.origin);
