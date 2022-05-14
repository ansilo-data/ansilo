import { VersionNumber } from "../components/catalog/catalog.slice";

export const versionLabel = (v: VersionNumber): string => {
  return `v${v.major}.${v.minor}.${v.patch}`;
};
