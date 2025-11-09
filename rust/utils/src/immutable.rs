#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RefList<'a, T> {
    #[default]
    Nil,

    Cons(T, &'a RefList<'a, T>),
}

impl<'a, T> RefList<'a, T> {
    pub fn prepend(&'a self, head: T) -> Self {
        Self::Cons(head, self)
    }

    pub fn iter(&'a self) -> impl Iterator<Item = &'a T> {
        self
    }

    pub fn head(&'a self) -> Option<&'a T> {
        match self {
            RefList::Nil => None,
            RefList::Cons(head, _) => Some(head),
        }
    }

    pub fn headn(&'a self, n: usize) -> Option<&'a T> {
        match self {
            RefList::Nil => None,
            RefList::Cons(head, tail) => {
                if n == 0 {
                    Some(head)
                } else {
                    tail.headn(n - 1)
                }
            }
        }
    }

    pub fn tail(&'a self) -> Option<&'a RefList<'a, T>> {
        match self {
            RefList::Nil => None,
            RefList::Cons(_, tail) => Some(tail),
        }
    }

    pub fn tailn(&'a self, n: usize) -> Option<&'a RefList<'a, T>> {
        if n == 0 {
            Some(self)
        } else {
            match self {
                RefList::Nil => None,
                RefList::Cons(_, tail) => tail.tailn(n - 1),
            }
        }
    }
}

impl<'a, T> Iterator for &'a RefList<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        let current = *self;
        match current {
            RefList::Nil => None,
            RefList::Cons(head, tail) => {
                *self = *tail;
                Some(head)
            }
        }
    }
}
