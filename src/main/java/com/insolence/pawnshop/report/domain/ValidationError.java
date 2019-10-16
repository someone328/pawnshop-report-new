package com.insolence.pawnshop.report.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@ToString
public class ValidationError {

    private String errorText;

}
