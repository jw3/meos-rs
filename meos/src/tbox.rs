use std::cmp::Ordering;
use std::fmt::{Debug, Formatter, Write};
use std::ptr::NonNull;

use libc::free;

use meos_sys as ffi;
use meos_sys::{
    contains_tbox_tbox, int_to_tbox, overlaps_tbox_tbox, same_tbox_tbox, tbox_cmp, tbox_eq,
    tbox_out,
};

use crate::{to_c_str, try_cstr_to_str};
use crate::error::Error;
use crate::error::Error::MeosError;

// todo;; TBox trait?

pub struct TBox {
    ptr: NonNull<ffi::TBox>,
}

impl Drop for TBox {
    fn drop(&mut self) {
        unsafe {
            println!("dropped");
            free(self.ptr.as_ptr().cast());
        }
    }
}

impl PartialEq<Self> for TBox {
    fn eq(&self, other: &Self) -> bool {
        unsafe { tbox_eq(self.ptr.as_ptr().cast(), other.ptr.as_ptr().cast()) }
    }
}

impl Eq for TBox {}

impl PartialOrd<Self> for TBox {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TBox {
    fn cmp(&self, other: &Self) -> Ordering {
        unsafe {
            match tbox_cmp(self.ptr.as_ptr().cast(), other.ptr.as_ptr().cast()) {
                -1 => Ordering::Less,
                0 => Ordering::Equal,
                1 => Ordering::Greater,
                v => unreachable!("tbox_cmp returned {}", v),
            }
        }
    }
}

impl Debug for TBox {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.as_wkt())
    }
}

impl TBox {
    pub fn as_wkt(&self) -> String {
        unsafe {
            let cstr = tbox_out(self.ptr.as_ptr().cast(), 6);
            try_cstr_to_str(cstr).expect("box as_wkt")
        }
    }

    pub fn from_wkt(wkt: &str) -> Result<Self, Error> {
        unsafe {
            let cstr = to_c_str(wkt)?;
            let ptr = ffi::tbox_in(cstr.as_ptr());
            if ptr.is_null() {
                // todo;; check the meos error
                return Err(MeosError(-999));
            }
            Ok(Self {
                ptr: NonNull::new(ptr).unwrap().cast(),
            })
        }
    }

    pub fn from_int(i: i32) -> Self {
        unsafe {
            let ptr = int_to_tbox(i);
            if ptr.is_null() {
                // todo;; check the meos error
                panic!("meos internal error")
            }
            Self {
                ptr: NonNull::new(ptr).unwrap().cast(),
            }
        }
    }

    pub fn contains(&self, other: &Self) -> bool {
        unsafe { contains_tbox_tbox(self.ptr.as_ptr(), other.ptr.as_ptr()) }
    }

    pub fn overlaps(&self, other: &Self) -> bool {
        unsafe { overlaps_tbox_tbox(self.ptr.as_ptr(), other.ptr.as_ptr()) }
    }

    pub fn same(&self, other: &Self) -> bool {
        unsafe { same_tbox_tbox(self.ptr.as_ptr(), other.ptr.as_ptr()) }
    }
}

pub struct STBox {
    ptr: NonNull<ffi::TBox>,
}

impl Drop for STBox {
    fn drop(&mut self) {
        unsafe {
            free(self.ptr.as_ptr().cast());
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{finalize, init};
    use crate::tbox::TBox;

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
        let a = "TBOX X([1.1, 4.0))";
        let o = "TBOXFLOAT X([1.1, 4))";
        let b = TBox::from_wkt(a).unwrap().as_wkt();
        assert_eq!(o, &b);

        let a = "TBOX XT((4, 12),[2001-01-01, 2001-10-01])";
        let o = "TBOXFLOAT XT((4, 12),[2001-01-01 00:00:00-05, 2001-10-01 00:00:00-04])";
        let b = TBox::from_wkt(a).unwrap().as_wkt();
        assert_eq!(o, &b);

        let a = "TBOX XT([1.0, 4.0),[2001-01-01, 2001-01-02])";
        let o = "TBOXFLOAT XT([1, 4),[2001-01-01 00:00:00-05, 2001-01-02 00:00:00-05])";
        let b = TBox::from_wkt(a).unwrap().as_wkt();
        assert_eq!(o, &b);

        let a = "TBOX T([2001-01-01, 2001-01-02])";
        let o = "TBOX T([2001-01-01 00:00:00-05, 2001-01-02 00:00:00-05])";
        let b = TBox::from_wkt(a).unwrap().as_wkt();
        assert_eq!(o, &b);
    }

    #[test]
    fn test_tbox_contains() {
        let a = TBox::from_wkt("TBOX T([2001-01-01, 2001-01-02])").unwrap();
        let b = TBox::from_wkt("TBOX T([2001-01-01, 2001-01-02])").unwrap();
        assert!(a.contains(&b));

        let a = TBox::from_wkt("TBOX T([2001-01-01, 2001-01-02])").unwrap();
        let b = TBox::from_wkt("TBOX T([2001-01-01, 2001-01-03])").unwrap();
        assert!(!a.contains(&b));
    }

    #[test]
    fn test_tbox_overlaps() {
        let a = TBox::from_wkt("TBOX T([2001-01-01, 2001-01-02])").unwrap();
        let b = TBox::from_wkt("TBOX T([2001-01-01, 2001-01-02])").unwrap();
        assert!(a.overlaps(&b));

        let a = TBox::from_wkt("TBOX T([2001-01-01, 2001-01-02])").unwrap();
        let b = TBox::from_wkt("TBOX T([2001-01-01, 2001-01-03])").unwrap();
        assert!(a.overlaps(&b));
    }

    #[test]
    fn test_tbox_same() {
        let a = TBox::from_wkt("TBOX T([2001-01-01, 2001-01-02])").unwrap();
        let b = TBox::from_wkt("TBOX T([2001-01-01, 2001-01-02])").unwrap();
        assert!(a.same(&b));

        let a = TBox::from_wkt("TBOX T([2001-01-01, 2001-01-02])").unwrap();
        let b = TBox::from_wkt("TBOX T([2001-01-01, 2002-01-02])").unwrap();
        assert!(!a.same(&b));
    }

    #[test]
    fn test_tbox_eq() {
        let a = TBox::from_wkt("TBOX T([2001-01-01, 2001-01-02])").unwrap();
        let b = TBox::from_wkt("TBOX T([2001-01-01, 2001-01-02])").unwrap();
        assert_eq!(a, b);

        let a = TBox::from_wkt("TBOX T([2001-01-02, 2001-01-02])").unwrap();
        let b = TBox::from_wkt("TBOX T([2001-01-01, 2001-01-02])").unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn test_tbox_int() {
        let b = TBox::from_int(1);
    }
}
