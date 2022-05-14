import * as React from "react";
import { useAppDispatch, useAppSelector } from "../../store/hooks";
import {
  EntitySchemaVersion,
  fetchCatalogAsync,
  selectCatalog,
} from "./catalog.slice";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TablePagination from "@mui/material/TablePagination";
import TableRow from "@mui/material/TableRow";
import { styled } from "@mui/material/styles";
import Typography from "@mui/material/Typography";

interface Props {
  version: EntitySchemaVersion;
}

export default function EntityAttributesTable(props: Props) {
  return (
    <TableContainer>
      <Table stickyHeader>
        <TableHead>
          <TableRow>
            <TableCell>Attribute</TableCell>
            <TableCell>Data Type</TableCell>
            <TableCell>Constraints</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {props.version.attributes.map((a) => (
            <TableRow key={a.id}>
              <TableCell>
                {a.name}
                <br />
                <Typography
                  variant="subtitle2"
                  sx={{ fontSize: 12, color: "text.secondary" }}
                >
                  {a.description}
                </Typography>
              </TableCell>
              <TableCell>{a.type.name}</TableCell>
              <TableCell>
                <ul>
                  {a.constraints?.map((i) => (
                    <li key={i.name}>{i.name}</li>
                  ))}
                  {a.validations?.map((i) => (
                    <li key={i.name}>{i.name}</li>
                  ))}
                </ul>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableContainer>
  );
}
