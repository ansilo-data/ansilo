import { useInterval } from "../../util/useInterval";
import { useAppDispatch, useAppSelector } from "../../store/hooks";
import { fetchCatalogAsync, selectCatalog } from "./catalog.slice";
import { useEffect } from "react";

export const CatalogDataRefresh = () => {
  const dispatch = useAppDispatch();
  const catalog = useAppSelector(selectCatalog);

  useInterval(() => {
    if (catalog.status !== "loading") {
      dispatch(fetchCatalogAsync());
    }
  }, 30000);

  useEffect(() => {
    dispatch(fetchCatalogAsync());
  }, [dispatch]);

  return <></>;
};
