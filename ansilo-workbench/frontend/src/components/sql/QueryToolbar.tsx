import Paper from "@mui/material/Paper";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import { CatalogState } from "../catalog/catalog.slice";
import LoadingButton from "@mui/lab/LoadingButton";
import { QueryStatus } from "./sql.slice";

interface Props {
  queryStatus: QueryStatus;
  onExecute: () => void;
}

export const QueryToolbar = (props: Props) => {
  return (
    <Box sx={{ display: "flex", flexDirection: "row" }}>
      <LoadingButton
        variant="contained"
        loading={props.queryStatus === QueryStatus.Executing}
        disabled={props.queryStatus === QueryStatus.Executing}
        onClick={() => props.onExecute()}
      >
        Execute
      </LoadingButton>
    </Box>
  );
};
