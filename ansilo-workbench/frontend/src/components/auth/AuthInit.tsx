import { loadInitialState } from "./auth.slice";
import { useEffect } from "react";
import { useAppDispatch } from "../../store/hooks";

export const AuthInit = () => {
  const dispatch = useAppDispatch();

  useEffect(() => {
    dispatch(loadInitialState(null))
  }, []);

  return <></>
};
