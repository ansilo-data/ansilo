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

export const QueryEditor = ({ catalog, sql, onChange, ...props }: Props) => {
  return (
    <StyledContainer>
      <CodeMirror
        height="100%"
        width="100%"
        theme={darcula}
        extensions={[sqlExtension({
          dialect: PostgreSQL,
          schema: getSchema(catalog),
          defaultSchema: 'public',
        })]}
        value={sql}
        onChange={(value, viewUpdate) => onChange(value)}
        {...props}
      />
    </StyledContainer>
  );
};

const getSchema = (catalog: CatalogState): { [table: string]: string[] } => {
  let schema = {} as any;

  for (const entity of catalog.nodes?.flatMap(n => n.schema.entities.flatMap(e => e.versions)) || []) {
    schema[`public.${entity.tableName}`] = entity.attributes.map(a => a.name);
  }

  return schema;
}