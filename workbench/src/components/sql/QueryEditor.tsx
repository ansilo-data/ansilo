import { styled } from "@mui/material/styles";
import TextField from "@mui/material/TextField";
import { CatalogState } from "../catalog/catalog.slice";

interface Props {
  required?: boolean;
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

export const QueryEditor = ({ sql, onChange, ...props }: Props) => {
  return (
    <StyledTextField
      label="SQL Editor"
      placeholder="SELECT * FROM your_dataset..."
      multiline
      value={sql}
      onChange={(e) => onChange(e.target.value)}
      {...props}
    />
  );
};
