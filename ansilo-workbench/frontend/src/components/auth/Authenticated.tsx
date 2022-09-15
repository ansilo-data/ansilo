import { useEffect } from "react";
import { useAppDispatch, useAppSelector } from "../../store/hooks";
import { incrementAuthRequired, decrementAuthRequired } from "./auth.slice";

export const Authenticated = ({ children }: { children: any }) => {
  const dispatch = useAppDispatch()

  useEffect(() => {
    dispatch(incrementAuthRequired(null))

    return () => {
      dispatch(decrementAuthRequired(null))
    }
  }, [])

  return (
    <>
      {children}
    </>
  );
};
