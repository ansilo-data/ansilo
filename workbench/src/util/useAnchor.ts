import { useEffect, useRef, useState } from "react";

export const useAnchor = (
  initialState?: string
): [string, (v: string) => void] => {
  const [anchorState, setAnchorState] = useState<string>(
    global?.window?.location?.hash?.substring(1)
  );

  useEffect(() => {
    const handleChange = () => {
      setAnchorState(window.location.hash.substring(1));
    };

    window.addEventListener("hashchange", handleChange);
    return () => window.removeEventListener("hashchange", handleChange);
  }, []);

  const setAnchor = (v?: string) => {
    window.location.hash = v ? "#" + v : "";
  };

  return [anchorState, setAnchor];
};
