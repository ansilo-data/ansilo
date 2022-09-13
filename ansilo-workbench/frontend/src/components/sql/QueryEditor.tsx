import { styled } from "@mui/material/styles";
import CodeMirror from '@uiw/react-codemirror';
import { sql as sqlExtension, PostgreSQL } from "@codemirror/lang-sql"
import { darcula } from '@uiw/codemirror-theme-darcula'

import { CatalogState } from "../catalog/catalog.slice";

interface Props {
  required?: boolean;
  catalog: CatalogState;
  sql: string;
  onChange: (sql: string) => void;
}

const StyledContainer = styled(
  'div',
  {}
)({
  "&": {
    width: "100%",
    height: "100%",
    borderRadius: 3,
    boxShadow: '0 0 2px #333'
  },
  "& > *": {
    width: "100%",
    height: "100%",
  },
});

export const QueryEditor = ({ sql, onChange, ...props }: Props) => {
  return (
    <StyledContainer>
      <CodeMirror
        height="100%"
        width="100%"
        theme={darcula}
        extensions={[sqlExtension({
          dialect: PostgreSQL,
        })]}
        value={sql}
        onChange={(value, viewUpdate) => onChange(value)}
        {...props}
      />
    </StyledContainer>
  );
};
