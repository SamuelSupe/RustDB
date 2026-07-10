use std::ops::ControlFlow;

use sqlparser::ast::{TableFactor, Visit, VisitMut, Visitor, VisitorMut};

use crate::{Error, Result};

pub(super) fn visit(
    statement: &sqlparser::ast::Statement,
    visitor: &mut impl FnMut(&TableFactor) -> Result<()>,
) -> Result<()> {
    struct Adapter<'a, F>(&'a mut F);

    impl<F> Visitor for Adapter<'_, F>
    where
        F: FnMut(&TableFactor) -> Result<()>,
    {
        type Break = Error;

        fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<Self::Break> {
            match (self.0)(factor) {
                Ok(()) => ControlFlow::Continue(()),
                Err(error) => ControlFlow::Break(error),
            }
        }
    }

    match statement.visit(&mut Adapter(visitor)) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(error) => Err(error),
    }
}

pub(super) fn visit_mut(
    statement: &mut sqlparser::ast::Statement,
    visitor: &mut impl FnMut(&mut TableFactor) -> Result<()>,
) -> Result<()> {
    struct Adapter<'a, F>(&'a mut F);

    impl<F> VisitorMut for Adapter<'_, F>
    where
        F: FnMut(&mut TableFactor) -> Result<()>,
    {
        type Break = Error;

        fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<Self::Break> {
            match (self.0)(factor) {
                Ok(()) => ControlFlow::Continue(()),
                Err(error) => ControlFlow::Break(error),
            }
        }
    }

    match statement.visit(&mut Adapter(visitor)) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(error) => Err(error),
    }
}
