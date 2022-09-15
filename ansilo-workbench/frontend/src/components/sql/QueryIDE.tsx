import Paper from "@mui/material/Paper";
import Box from "@mui/material/Box";
import { useAppDispatch, useAppSelector } from "../../store/hooks";
import {
  executeCurrentQueryAsync,
  selectSql,
  updateCurrentQuery,
} from "./sql.slice";
import { QueryEditor } from "./QueryEditor";
import { selectCatalog } from "../catalog/catalog.slice";
import { QueryToolbar } from "./QueryToolbar";
import { QueryResults } from "./QueryResults";
import { QueryHistory } from "./QueryHistory";
import { Authenticated } from "../auth/Authenticated";

export const QueryIDE = () => {
  const dispatch = useAppDispatch();
  const catalog = useAppSelector(selectCatalog);
  const sql = useAppSelector(selectSql);

  return (
    <Authenticated>
      <Box
        sx={{
          display: "flex",
          flexDirection: "row",
          height: "100%",
          width: '100%'
        }}
      >
        <Box
          sx={{
            display: "flex",
            flexDirection: "column",
            height: "100%",
            width: 'calc(100% - 340px)',
          }}
        >
          <Box
            sx={{
              display: "flex",
              height: "40%",
              minHeight: "300px",
              width: "100%",
              overflowX: "auto",
              p: 2,
            }}
          >
            <QueryEditor
              catalog={catalog}
              sql={sql.currentQuery.sql}
              onChange={(sql) => dispatch(updateCurrentQuery(sql))}
            />
          </Box>
          <Paper
            elevation={4}
            sx={{
              borderRadius: 0,
              zIndex: 1,
              display: "flex",
              width: "100%",
              height: 50,
              px: 2,
              py: 1,
            }}
          >
            <QueryToolbar
              queryStatus={sql.currentQuery.status}
              onExecute={() => dispatch(executeCurrentQueryAsync())}
            />
          </Paper>
          <Box
            sx={{
              display: "flex",
              width: "100%",
              height: "calc(60% - 50px)",
              flexGrow: 1,
              flexShrink: 0,
              overflowY: "scroll",
              p: 2,
            }}
          >
            <QueryResults query={sql.currentQuery} />
          </Box>
        </Box>
        <Paper
          elevation={8}
          sx={{ width: 340, height: "100%", overflowY: "scroll", zIndex: 10 }}
        >
          <QueryHistory
            pastQueries={sql.queryHistory}
            onClick={(query) => dispatch(updateCurrentQuery(query.sql))}
          />
        </Paper>
      </Box>
    </Authenticated>
  );
};
