import { styled } from "@mui/material/styles";
import TextField from "@mui/material/TextField";
import { CatalogState } from "../catalog/catalog.slice";

interface Props {
  catalog: CatalogState;
  sql: string;
  onChange: (sql: string) => void;
}

const StyledTextField = styled(
  TextField,
  {}
)({
  "&": {
    width: "100%",
    height: "100%",
  },
  "& .MuiInputBase-root": {
    width: "100%",
    height: "100%",
  },
  "& textarea": {
    width: "100%",
    height: "100%!important",
  },
});

export const QueryEditor = (props: Props) => {
  return (
    <StyledTextField
      label="SQL Editor"
      placeholder="SELECT * FROM your_dataset..."
      multiline
      value={props.sql}
      onChange={(e) => props.onChange(e.target.value)}
    />
  );
};
