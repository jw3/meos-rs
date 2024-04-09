use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::ptr::NonNull;

use libc::free;

use meos_sys as ffi;
use meos_sys::{
    contains_stbox_tpoint, overlaps_stbox_stbox, same_stbox_stbox, stbox_cmp, stbox_eq, stbox_out,
};

use crate::error::Error;
use crate::error::Error::MeosError;
use crate::{to_c_str, try_cstr_to_str};

pub struct STBox {
    ptr: NonNull<ffi::STBox>,
}

impl Drop for STBox {
    fn drop(&mut self) {
        unsafe {
            free(self.ptr.as_ptr().cast());
        }
    }
}

impl PartialEq<Self> for STBox {
    fn eq(&self, other: &Self) -> bool {
        unsafe { stbox_eq(self.ptr.as_ptr().cast(), other.ptr.as_ptr().cast()) }
    }
}

impl Eq for STBox {}

impl PartialOrd<Self> for STBox {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for STBox {
    fn cmp(&self, other: &Self) -> Ordering {
        unsafe {
            match stbox_cmp(self.ptr.as_ptr().cast(), other.ptr.as_ptr().cast()) {
                -1 => Ordering::Less,
                0 => Ordering::Equal,
                1 => Ordering::Greater,
                v => unreachable!("tbox_cmp returned {}", v),
            }
        }
    }
}

impl Debug for STBox {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.as_wkt())
    }
}

impl STBox {
    pub fn as_wkt(&self) -> String {
        unsafe {
            let cstr = stbox_out(self.ptr.as_ptr().cast(), 6);
            try_cstr_to_str(cstr).expect("box as_wkt")
        }
    }

    pub fn from_wkt(wkt: &str) -> Result<Self, Error> {
        unsafe {
            let cstr = to_c_str(wkt)?;
            let ptr = ffi::stbox_in(cstr.as_ptr());
            if ptr.is_null() {
                // todo;; check the meos error
                return Err(MeosError(-999));
            }
            Ok(Self {
                ptr: NonNull::new(ptr).unwrap().cast(),
            })
        }
    }

    pub fn contains(&self, other: &Self) -> bool {
        unsafe { contains_stbox_tpoint(self.ptr.as_ptr().cast(), other.ptr.as_ptr().cast()) }
    }

    pub fn overlaps(&self, other: &Self) -> bool {
        unsafe { overlaps_stbox_stbox(self.ptr.as_ptr(), other.ptr.as_ptr()) }
    }

    pub fn same(&self, other: &Self) -> bool {
        unsafe { same_stbox_stbox(self.ptr.as_ptr(), other.ptr.as_ptr()) }
    }
}

#[cfg(test)]
mod tests {
    use crate::stbox::STBox;
    use crate::{finalize, init};

    #[cfg(test)]
    #[ctor::ctor]
    fn meos_init() {
        init();
    }

    #[cfg(test)]
    #[ctor::dtor]
    fn meos_finalize() {
        finalize();
    }

    #[test]
    fn test_tbox_wkt() {
        let a = "STBOX X((1.0, 2.0), (3.0, 4.0))";
        let o = "STBOX X((1,2),(3,4))";
        let b = STBox::from_wkt(a).unwrap().as_wkt();
        assert_eq!(o, b);

        let a = "STBOX Z((1.0, 2.0, 3.0), (4.0, 5.0, 6.0))";
        let o = "STBOX Z((1,2,3),(4,5,6))";
        let b = STBox::from_wkt(a).unwrap().as_wkt();
        assert_eq!(o, b);
    }
}
